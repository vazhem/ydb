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
#include <ydb/library/actors/wilson/wilson_trace.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/blobstorage_ddisk_config.pb.h>

#include <util/generic/hash.h>
#include <util/generic/queue.h>
#include <memory>

namespace NKikimr {

// Forward declaration
struct TDDiskWorkerConfig;

////////////////////////////////////////////////////////////////////////////////

//
// DDisk events for direct operations (offset/size based)
//
// DDisk events are now defined in blobstorage.h as part of TEvBlobStorage

////////////////////////////////////////////////////////////////////////////////

//
// DDisk actor base abstract class for ErasureMirror3Direct
//
class TDDiskActorImpl
    : public NActors::TActorBootstrapped<TDDiskActorImpl>
{
public:
    // DDisk actor states
    enum class EDDiskState {
        Init,   // Initializing, PDisk not ready
        Ready   // PDisk ready, workers created
    };

protected:
    TIntrusivePtr<TVDiskConfig> Config;
    TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
    TVDiskID SelfVDiskId;
    TPDiskCtxPtr PDiskCtx;

    // Actor state management
    EDDiskState CurrentState;

    // PDisk initialization
    bool PDiskInitialized;
    ui32 ChunkSize;  // PDisk chunk size received during initialization

    // Block device interface for direct I/O (used by DIRECT_IO mode)
    NPDisk::IBlockDevice* BlockDevice;
    TString DevicePath;

    // Worker pool management
    static constexpr ui32 DEFAULT_WORKER_COUNT = 1;
    TVector<TActorId> WorkerActors;
    ui32 NextWorkerIndex;
    ui32 WorkerCount = 1;  // Configurable via ddisk_config in config.yaml

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
        NWilson::TSpan Span;  // Wilson tracing span for this request

        TPendingRequest() = default;
        TPendingRequest(ui32 offset,
            ui32 size,
            ui32 chunkId,
            TActorId sender,
            ui64 cookie,
            bool isWrite,
            const TString& writeData = "",
            ui64 originalRequestId = 0,
            bool isDirectIO = false,
            NWilson::TSpan span = NWilson::TSpan())
            : Offset(offset), Size(size), ChunkId(chunkId), Sender(sender), Cookie(cookie), IsWrite(isWrite), WriteData(writeData), OriginalRequestId(originalRequestId), IsDirectIO(isDirectIO), Span(std::move(span)) {}
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

public:
    // DDisk operation mode
    enum class EDDiskMode {
        MEMORY,         // In-memory storage for debugging
        PDISK_EVENTS,   // Use PDisk events (traditional PDisk interface)
        DIRECT_IO       // Direct device I/O to chunks (uses PDisk PwriteAsync, PreadAsync)
    };

    EDDiskMode Mode;

    // Output operator for EDDiskMode
    friend IOutputStream& operator<<(IOutputStream& out, EDDiskMode mode) {
        switch (mode) {
            case EDDiskMode::MEMORY:
                return out << "Memory";
            case EDDiskMode::PDISK_EVENTS:
                return out << "PDisk Events";
            case EDDiskMode::DIRECT_IO:
                return out << "Direct IO";
            default:
                return out << "Unknown";
        }
    }

    // Chunk reservation info from PDisk (for device access)
    struct TChunkInfo {
        ui64 DeviceOffset;  // Physical offset on device
        bool IsReserved;

        TChunkInfo() : DeviceOffset(0), IsReserved(false) {}
        TChunkInfo(ui64 offset) : DeviceOffset(offset), IsReserved(true) {}
    };
    THashMap<ui32, TChunkInfo> ChunkInfoMap;  // chunkId -> device offset

    // Virtual methods for different modes - to be implemented by derived classes
    virtual void ProcessReadRequest(
        const TEvBlobStorage::TEvDDiskReadRequest::TPtr& ev,
        const NActors::TActorContext& ctx) = 0;

    virtual void ProcessWriteRequest(
        const TEvBlobStorage::TEvDDiskWriteRequest::TPtr& ev,
        const NActors::TActorContext& ctx) = 0;

    virtual void HandleChunkReadResult(
        const NPDisk::TEvChunkReadResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    virtual void HandleChunkWriteResult(
        const NPDisk::TEvChunkWriteResult::TPtr& ev,
        const NActors::TActorContext& ctx);

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_SKELETON_FRONT;
    }

    TDDiskActorImpl(
        TIntrusivePtr<TVDiskConfig> cfg,
        TIntrusivePtr<TBlobStorageGroupInfo> info,
        EDDiskMode mode,
        const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
        ui32 workerCount,
        ui32 chunksPerReservation)
        : Config(std::move(cfg))
        , GInfo(std::move(info))
        , PDiskCtx(nullptr)
        , CurrentState(EDDiskState::Init)
        , PDiskInitialized(false)
        , ChunkSize(0)  // Will be set during PDisk initialization
        , BlockDevice(nullptr)  // Will be set during PDisk initialization for DIRECT_IO mode
        , NextWorkerIndex(0)
        , WorkerCount(workerCount > 0 ? workerCount : DEFAULT_WORKER_COUNT)
        , ChunkReservationInProgress(false)
        , ChunksPerReservation(chunksPerReservation > 0 ? chunksPerReservation : 10)
        , Mode(mode)
    {
        Y_UNUSED(counters);

        LOG_INFO_S(TActivationContext::AsActorContext(), NKikimrServices::BS_DDISK,
            "DDisk actor created with WorkerCount=" << WorkerCount
            << ", ChunksPerReservation=" << ChunksPerReservation
            << ", Mode=" << (ui32)Mode);
    }

    void Bootstrap(const NActors::TActorContext& ctx);

    // Factory method to create appropriate instance based on mode
    static std::unique_ptr<TDDiskActorImpl> Create(
        TIntrusivePtr<TVDiskConfig> cfg,
        TIntrusivePtr<TBlobStorageGroupInfo> info,
        EDDiskMode mode,
        const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
        ui32 workerCount,
        ui32 chunksPerReservation);

protected:
    // Common handlers
    void HandleReserveChunksRequest(
        const TEvBlobStorage::TEvDDiskReserveChunksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleChunkReserveResult(
        const NPDisk::TEvChunkReserveResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleYardInitResult(
        const NPDisk::TEvYardInitResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePing(
        const TEvBlobStorage::TEvDDiskPing::TPtr& ev,
        const NActors::TActorContext& ctx);

    // Helper methods
    void InitializePDisk(const NActors::TActorContext& ctx);
    void SendErrorResponse(const TPendingRequest& request, const TString& errorReason, const NActors::TActorContext& ctx);

    // State transition methods
    void TransitionToReady(const NActors::TActorContext& ctx);

    // Worker pool management
    void CreateWorkerPool(const NActors::TActorContext& ctx);
    TActorId SelectNextWorker();
    TDDiskWorkerConfig CreateWorkerConfig() const;
    void BroadcastChunkInfoUpdate(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork)
    {
        LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::BS_DDISK,
            "🔥 DDISK STATEWORK: Received event type=" << ev->GetTypeRewrite()
            << " sender=" << ev->Sender.ToString()
            << " cookie=" << ev->Cookie
            << " state=" << (ui32)CurrentState);

        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvDDiskReadRequest, HandleReadRequest);
            HFunc(TEvBlobStorage::TEvDDiskWriteRequest, HandleWriteRequest);
            HFunc(TEvBlobStorage::TEvDDiskReserveChunksRequest, HandleReserveChunksRequest);
            HFunc(TEvBlobStorage::TEvDDiskPing, HandlePing);
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

    virtual void HandleReadRequest(
        const TEvBlobStorage::TEvDDiskReadRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    virtual void HandleWriteRequest(
        const TEvBlobStorage::TEvDDiskWriteRequest::TPtr& ev,
        const NActors::TActorContext& ctx);
};

// Forward declarations for mode-specific implementations
class TDDiskMemoryActor;
class TDDiskPDiskEventsActor;
class TDDiskDirectIOActor;

}   // namespace NKikimr
