#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>
#include <ydb/core/blobstorage/ddisk/ddisk_events.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_blockdevice.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_completion.h>

#include <util/generic/hash.h>
#include <util/generic/queue.h>

namespace NKikimr {

////////////////////////////////////////////////////////////////////////////////

//
// DDisk events for direct operations (offset/size based)
//
// DDisk events are now defined in blobstorage.h as part of TEvBlobStorage

////////////////////////////////////////////////////////////////////////////////

//
// DDisk actor implementation for ErasureMirror3Direct
//
class TDDiskActorImpl final
    : public NActors::TActorBootstrapped<TDDiskActorImpl>
{
private:
    TIntrusivePtr<TVDiskConfig> Config;
    TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
    TVDiskID SelfVDiskId;
    TPDiskCtxPtr PDiskCtx;

    // PDisk initialization
    bool PDiskInitialized;
    ui32 ChunkSize;  // PDisk chunk size received during initialization

    // Chunk management - track owned chunks for validation
    THashSet<ui32> KnownChunks;

    // Pending operations
    struct TPendingRequest {
        ui32 Offset;  // ui32 for chunk-relative offset
        ui32 Size;
        ui32 ChunkId;
        TActorId Sender;
        ui64 Cookie;
        bool IsWrite;
        TString WriteData;  // For write requests
        ui64 OriginalRequestId;  // Original request ID for traceability
        bool IsDirectIO;  // Flag to track if this is a DirectIO request

        TPendingRequest() = default;
        TPendingRequest(ui32 offset, ui32 size, ui32 chunkId, TActorId sender, ui64 cookie, bool isWrite, const TString& writeData = "", ui64 originalRequestId = 0, bool isDirectIO = false)
            : Offset(offset), Size(size), ChunkId(chunkId), Sender(sender), Cookie(cookie), IsWrite(isWrite), WriteData(writeData), OriginalRequestId(originalRequestId), IsDirectIO(isDirectIO) {}
    };

    THashMap<void*, TPendingRequest> PendingRequests;
    ui64 NextRequestCookie = 1;  // Incrementing counter for unique cookies

    // Pending chunk reservation requests
    struct TPendingChunkReservation {
        TActorId Sender;
        ui64 Cookie;
        ui32 ChunkCount;

        TPendingChunkReservation() = default;
        TPendingChunkReservation(TActorId sender, ui64 cookie, ui32 chunkCount)
            : Sender(sender), Cookie(cookie), ChunkCount(chunkCount) {}
    };

    THashMap<void*, TPendingChunkReservation> PendingReservations;

    // Current chunk allocation state
    bool ChunkReservationInProgress;
    ui32 ChunksPerReservation;

    // IN-MEMORY STORAGE IMPLEMENTATION FOR DEBUGGING
    // This bypasses PDisk entirely to isolate corruption issues
    bool UseInMemoryStorage = false;

    // In-memory chunk data structure
    struct TChunkData {
        TString Data;

        TChunkData() = default;
        TChunkData(const TString& data) : Data(data) {}

        TString ReadData(ui32 offset, ui32 size) const {
            if (offset >= Data.size()) {
                return TString(size, '\0');
            }
            ui32 availableSize = Min(size, static_cast<ui32>(Data.size()) - offset);
            return Data.substr(offset, availableSize) + TString(size - availableSize, '\0');
        }

        void WriteData(ui32 offset, const TString& data) {
            if (offset + data.size() > Data.size()) {
                Data.resize(offset + data.size());
            }
            memcpy(const_cast<char*>(Data.data()) + offset, data.data(), data.size());
        }
    };

    THashMap<ui32, TChunkData> InMemoryChunks;

    // DDiskWriter Direct Device I/O (bypasses PDisk)
    TActorId DDiskWriterId;
    TString DevicePath;
    bool UseDirectDeviceIO = true;  // Use direct device I/O instead of PDisk
    NPDisk::IBlockDevice* BlockDevice = nullptr;  // PDisk's block device interface for direct I/O


    // Chunk reservation info from PDisk (for device access)
    struct TChunkInfo {
        ui64 DeviceOffset;  // Physical offset on device
        bool IsReserved;

        TChunkInfo() : DeviceOffset(0), IsReserved(false) {}
        TChunkInfo(ui64 offset) : DeviceOffset(offset), IsReserved(true) {}
    };
    THashMap<ui32, TChunkInfo> ChunkInfoMap;  // chunkId -> device offset

    // Simple direct I/O completion handler that doesn't interfere with PDisk buffer management
    class TDirectIOCompletion : public NPDisk::TCompletionAction {
    private:
        TActorId DDiskActorId;  // DDisk actor ID for logging
        TActorId OriginalSender;  // The original request sender
        ui64 OriginalCookie;     // The original request cookie
        ui64 RequestId;          // The request ID for traceability
        TChunkIdx ChunkIdx;
        ui32 OriginalOffset;
        ui32 OriginalSize;
        ui32 OffsetAdjustment;
        bool IsRead;
        char* AlignedBuffer;  // Simple aligned buffer using aligned_alloc
        ui32 AlignedSize;
        mutable std::atomic<bool> ExecutedOrReleased{false};

    public:
        TDirectIOCompletion(const TActorId& ddiskActorId, const TActorId& originalSender,
                           ui64 originalCookie, ui64 requestId, TChunkIdx chunkIdx, ui32 originalOffset,
                           ui32 originalSize, ui32 offsetAdjustment, bool isRead,
                           char* alignedBuffer, ui32 alignedSize, const NWilson::TTraceId& traceId = {})
            : DDiskActorId(ddiskActorId)
            , OriginalSender(originalSender)
            , OriginalCookie(originalCookie)
            , RequestId(requestId)
            , ChunkIdx(chunkIdx)
            , OriginalOffset(originalOffset)
            , OriginalSize(originalSize)
            , OffsetAdjustment(offsetAdjustment)
            , IsRead(isRead)
            , AlignedBuffer(alignedBuffer)
            , AlignedSize(alignedSize)
        {
            // Initialize base class TCompletionAction fields
            OperationIdx = 0;  // Will be set by PDisk when operation is scheduled
            SubmitTime = 0;
            FlushAction = nullptr;
            CostNs = 0;
            Result = NPDisk::EIoResult::Unknown;
            ErrorReason = "";
            TraceId = NWilson::TTraceId(traceId);
        }

        virtual ~TDirectIOCompletion() {
            if (AlignedBuffer) {
                free(AlignedBuffer);
                AlignedBuffer = nullptr;
            }
        }

        char* Data() const { return AlignedBuffer; }
        ui32 Size() const { return AlignedSize; }

        bool CanHandleResult() const override {
            // Always handle result, even for errors - we want to send responses
            return true;
        }

        void Exec(TActorSystem* actorSystem) override;
        void Release(TActorSystem* actorSystem) override;
    };

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_SKELETON_FRONT;
    }

    TDDiskActorImpl(
        TIntrusivePtr<TVDiskConfig> cfg,
        TIntrusivePtr<TBlobStorageGroupInfo> info,
        const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
        : Config(std::move(cfg))
        , GInfo(std::move(info))
        , PDiskCtx(nullptr)
        , PDiskInitialized(false)
        , ChunkSize(0)  // Will be set during PDisk initialization
        , ChunkReservationInProgress(false)
        , ChunksPerReservation(10)  // Reserve chunks in batches
    {
        Y_UNUSED(counters);
    }

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork)
    {
        LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::BS_DDISK,
            "🔥 DDISK STATEWORK: Received event type=" << ev->GetTypeRewrite()
            << " sender=" << ev->Sender.ToString()
            << " cookie=" << ev->Cookie);

        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvDDiskReadRequest, HandleReadRequest);
            HFunc(TEvBlobStorage::TEvDDiskWriteRequest, HandleWriteRequest);
            HFunc(TEvBlobStorage::TEvDDiskReserveChunksRequest, HandleReserveChunksRequest);
            HFunc(NPDisk::TEvYardInitResult, HandleYardInitResult);
            HFunc(NPDisk::TEvChunkReserveResult, HandleChunkReserveResult);
            HFunc(NPDisk::TEvChunkReadResult, HandleChunkReadResult);
            HFunc(NPDisk::TEvChunkWriteResult, HandleChunkWriteResult);

            CFunc(NActors::TEvents::TSystem::PoisonPill, Die);

            default:
                Y_ABORT("Unexpected event type: %" PRIx32 " event: %s",
                       ev->GetTypeRewrite(), ev->ToString().data());
        }
    }

    void HandleReadRequest(
        const TEvBlobStorage::TEvDDiskReadRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteRequest(
        const TEvBlobStorage::TEvDDiskWriteRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReserveChunksRequest(
        const TEvBlobStorage::TEvDDiskReserveChunksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleChunkReserveResult(
        const NPDisk::TEvChunkReserveResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleChunkReadResult(
        const NPDisk::TEvChunkReadResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleChunkWriteResult(
        const NPDisk::TEvChunkWriteResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleYardInitResult(
        const NPDisk::TEvYardInitResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    // Helper methods
    void InitializePDisk(const NActors::TActorContext& ctx);
    // Helper methods for chunk management (legacy - kept for backwards compatibility)
    void EnsureChunksAvailable(const NActors::TActorContext& ctx);

    // Helper methods for error handling
    void SendErrorResponse(const TPendingRequest& request, const TString& errorReason, const NActors::TActorContext& ctx);
};

}   // namespace NKikimr
