#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_blockdevice.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_util_devicemode.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_mon.h>
#include <ydb/core/blobstorage/ddisk/ddisk_actor_impl.h>
#include <ydb/core/blobstorage/ddisk/ddisk_events.h>

#include <util/generic/hash.h>
#include <memory>

namespace NKikimr {

////////////////////////////////////////////////////////////////////////////////

// Configuration data that workers need to operate independently
struct TDDiskWorkerConfig {
    TDDiskActorImpl::EDDiskMode Mode;
    TFileHandle *SharedFileHandle;  // Shared file handle from PDisk for creating RealBlockDevice
    TString DevicePath;  // Device path (for monitoring and debugging purposes)
    ui32 ChunkSize;
    TVDiskID SelfVDiskId;
    ui32 VDiskSlotId;

    // Thread-safe copy of chunk info map
    THashMap<ui32, TDDiskActorImpl::TChunkInfo> ChunkInfoMap;

    TDDiskWorkerConfig() = default;

    TDDiskWorkerConfig(const TDDiskWorkerConfig&) = default;
    TDDiskWorkerConfig& operator=(const TDDiskWorkerConfig&) = default;
};

////////////////////////////////////////////////////////////////////////////////

// DDisk worker actor - handles read/write operations
class TDDiskWorkerActor : public NActors::TActor<TDDiskWorkerActor> {
private:
    ui32 WorkerId;
    TDDiskWorkerConfig Config;

    // Worker's own RealBlockDevice instance (with improved locking)
    std::unique_ptr<NPDisk::IBlockDevice> BlockDevice;
    std::unique_ptr<TPDiskMon> PdiskMon;  // Monitoring for block device
    std::unique_ptr<TPDiskConfig> PdiskConfig;  // Config for monitoring

    // Pending operations tracking (similar to main actor)
    struct TPendingRequest {
        ui32 Offset;
        ui32 Size;
        ui32 ChunkId;
        TActorId Sender;
        ui64 Cookie;
        bool IsWrite;
        TString WriteData;
        ui64 OriginalRequestId;

        TPendingRequest() = default;
        TPendingRequest(ui32 offset, ui32 size, ui32 chunkId, TActorId sender, ui64 cookie, bool isWrite, const TString& writeData = "", ui64 originalRequestId = 0)
            : Offset(offset), Size(size), ChunkId(chunkId), Sender(sender), Cookie(cookie), IsWrite(isWrite), WriteData(writeData), OriginalRequestId(originalRequestId) {}
    };

    THashMap<void*, TPendingRequest> PendingRequests;
    ui64 NextRequestCookie = 1;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_SKELETON_FRONT;
    }

    TDDiskWorkerActor(ui32 workerId, const TDDiskWorkerConfig& config);

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvDDiskReadRequest, HandleReadRequest);
            HFunc(TEvBlobStorage::TEvDDiskWriteRequest, HandleWriteRequest);
            HFunc(TEvDDiskChunkInfoUpdate, HandleChunkInfoUpdate);

            CFunc(NActors::TEvents::TSystem::PoisonPill, Die);

            default:
                Y_ABORT("Unexpected event type in DDisk worker: %" PRIx32 " event: %s",
                       ev->GetTypeRewrite(), ev->ToString().data());
        }
    }

    void HandleReadRequest(
        const TEvBlobStorage::TEvDDiskReadRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteRequest(
        const TEvBlobStorage::TEvDDiskWriteRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleChunkInfoUpdate(
        const TEvDDiskChunkInfoUpdate::TPtr& ev,
        const NActors::TActorContext& ctx);

private:
    // Process direct I/O operations (based on current DDisk implementation)
    template<typename TRequest>
    void ProcessDirectIORequest(
        const typename TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleDirectIOCompletion(
        void* completionPtr,
        bool success,
        const TString& errorReason,
        const NActors::TActorContext& ctx);
};

// Factory function
NActors::IActor* CreateDDiskWorkerActor(ui32 workerId, const TDDiskWorkerConfig& config);

} // namespace NKikimr
