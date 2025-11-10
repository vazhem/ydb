#include "ddisk_actor_impl.h"
#include "ddisk_actor_mode_memory.h"
#include "ddisk_actor_mode_pdisk.h"
#include "ddisk_actor_mode_direct.h"
#include "ddisk_worker_actor.h"
#include "ddisk_events.h"

#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/library/pdisk_io/buffers.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_util_devicemode.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_mon.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_config.h>
#include <ydb/library/pdisk_io/sector_map.h>
#include <ydb/library/wilson_ids/wilson.h>

namespace NKikimr {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

// Factory method implementation
std::unique_ptr<TDDiskActorImpl> TDDiskActorImpl::Create(
    TIntrusivePtr<TVDiskConfig> cfg,
    TIntrusivePtr<TBlobStorageGroupInfo> info,
    EDDiskMode mode,
    const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
    ui32 workerCount,
    ui32 chunksPerReservation)
{
    switch (mode) {
        case EDDiskMode::MEMORY:
            return std::make_unique<TDDiskMemoryActor>(std::move(cfg), std::move(info), counters, workerCount, chunksPerReservation);
        case EDDiskMode::PDISK_EVENTS:
            return std::make_unique<TDDiskPDiskEventsActor>(std::move(cfg), std::move(info), counters, workerCount, chunksPerReservation);
        case EDDiskMode::DIRECT_IO:
            return std::make_unique<TDDiskDirectIOActor>(std::move(cfg), std::move(info), counters, workerCount, chunksPerReservation);
        default:
            Y_ABORT("Unknown DDisk mode: %d", static_cast<int>(mode));
    }
}

////////////////////////////////////////////////////////////////////////////////

// Base class implementation

void TDDiskActorImpl::Bootstrap(const NActors::TActorContext& ctx)
{
    SelfVDiskId = GInfo->GetVDiskId(Config->BaseInfo.VDiskIdShort);

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "🚀 DDISK ACTOR BOOTSTRAP: Starting for VDiskId=" << SelfVDiskId.ToString()
        << " VDiskSlotId=" << Config->BaseInfo.VDiskSlotId
        << " Mode=" << (ui32)Mode
        << " actorId=" << ctx.SelfID.ToString());

    // Initialize PDisk connection
    InitializePDisk(ctx);

    Become(&TThis::StateWork);
}

void TDDiskActorImpl::HandleReadRequest(
    const TEvBlobStorage::TEvDDiskReadRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    // Create span for tracing
    NWilson::TSpan span(TWilson::BlobStorage, std::move(ev->TraceId.Clone()), "DDisk.Read");

    // Forward to worker actor
    TActorId workerId = SelectNextWorker();

    // Extract info before releasing event (avoid accessing moved object)
    ui64 cookie = ev->Cookie;
    ui32 flags = ev->Flags;
    TActorId sender = ev->Sender;
    ui32 chunkId = ev->Get()->Record.GetChunkId();
    ui32 offset = ev->Get()->Record.GetOffset();
    ui32 size = ev->Get()->Record.GetSize();

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "Forwarding DDisk read request #" << cookie
        << " (chunkId=" << chunkId
        << ", offset=" << offset
        << ", size=" << size << ") to worker " << workerId.ToString()
        << " traceId=" << span.GetTraceId().GetHexTraceId());

    // Forward to worker using span's trace ID
    ctx.Send(new IEventHandle(workerId, sender, ev->Release().Release(),
        flags, cookie, nullptr, span.GetTraceId()));

    span.EndOk();
}

void TDDiskActorImpl::HandleWriteRequest(
    const TEvBlobStorage::TEvDDiskWriteRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    // Create span for tracing
    NWilson::TSpan span(TWilson::BlobStorage, std::move(ev->TraceId.Clone()), "DDisk.Write");

    // Forward to worker actor
    TActorId workerId = SelectNextWorker();

    // Extract info before releasing event (avoid accessing moved object)
    ui64 cookie = ev->Cookie;
    ui32 flags = ev->Flags;
    TActorId sender = ev->Sender;
    ui32 chunkId = ev->Get()->Record.GetChunkId();
    ui32 offset = ev->Get()->Record.GetOffset();
    ui32 size = ev->Get()->Record.GetSize();

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "Forwarding DDisk write request #" << cookie
        << " (chunkId=" << chunkId
        << ", offset=" << offset
        << ", size=" << size << ") to worker " << workerId.ToString()
        << " traceId=" << span.GetTraceId().GetHexTraceId());

    // Forward to worker using span's trace ID
    ctx.Send(new IEventHandle(workerId, sender, ev->Release().Release(),
        flags, cookie, nullptr, span.GetTraceId()));

    span.EndOk();
}

void TDDiskActorImpl::HandleReserveChunksRequest(
    const TEvBlobStorage::TEvDDiskReserveChunksRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const ui32 chunkCount = msg->Record.GetChunkCount();

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "DDiskActorImpl: HandleReserveChunksRequest chunkCount=" << chunkCount
        << " from=" << ev->Sender.ToString());

    if (!PDiskInitialized) {
        // PDisk not yet initialized, return error
        auto response = std::make_unique<TEvBlobStorage::TEvDDiskReserveChunksResponse>();
        response->Record.SetStatus(NKikimrProto::ERROR);
        response->Record.SetErrorReason("PDisk not initialized");
        ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
        return;
    }

    if (chunkCount == 0) {
        // Invalid request
        auto response = std::make_unique<TEvBlobStorage::TEvDDiskReserveChunksResponse>();
        response->Record.SetStatus(NKikimrProto::ERROR);
        response->Record.SetErrorReason("Invalid chunk count");
        ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
        return;
    }

    // Store original request info to respond later
    ui64 reservationCookie = static_cast<ui64>(PendingReservations.size() + 1000);
    PendingReservations[reinterpret_cast<void*>(reservationCookie)] = TPendingChunkReservation(ev->Sender, ev->Cookie, chunkCount);

    // Create a request to PDisk to reserve chunks with cookie
    // For block device storage, request raw chunks without metadata
    auto pdiskRequest = std::make_unique<NPDisk::TEvChunkReserve>(
        PDiskCtx->Dsk->Owner,
        PDiskCtx->Dsk->OwnerRound,
        chunkCount,
        reservationCookie  // Pass cookie to PDisk
    );

    // Send request to PDisk
    ctx.Send(PDiskCtx->PDiskId, pdiskRequest.release());
}

void TDDiskActorImpl::HandleChunkReserveResult(
    const NPDisk::TEvChunkReserveResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    void* cookie = reinterpret_cast<void*>(msg->Cookie);

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "DDiskActorImpl: HandleChunkReserveResult status=" << NKikimrProto::EReplyStatus_Name(msg->Status)
        << " chunks=" << msg->ChunkIds.size());

    // Find the original request
    auto it = PendingReservations.find(cookie);
    if (it == PendingReservations.end()) {
        LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
            "DDiskActorImpl: Received chunk reserve result for unknown request");
        return;
    }

    const TPendingChunkReservation& reservation = it->second;

    // Create response
    auto response = std::make_unique<TEvBlobStorage::TEvDDiskReserveChunksResponse>();
    response->Record.SetStatus(msg->Status);

    if (msg->Status == NKikimrProto::OK) {
        // Add chunk size to response
        response->Record.SetChunkSize(ChunkSize);

        // Save device info for direct I/O access
        if (msg->DevicePath) {
            DevicePath = msg->DevicePath;
            LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
                "DDiskActorImpl: Saved device path: " << DevicePath);
        }

        // Save block device interface for direct I/O access
        if (msg->BlockDevice) {
            BlockDevice = msg->BlockDevice;
            LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
                "DDiskActorImpl: Saved block device interface: " << (void*)BlockDevice);
        }

        // Add reserved chunks to known chunks for validation and store device offsets
        for (ui32 i = 0; i < msg->ChunkIds.size(); ++i) {
            ui32 chunkIdx = msg->ChunkIds[i];
            KnownChunks.insert(chunkIdx);
            response->Record.AddChunkIds(chunkIdx);

            // Store device offset info for direct I/O
            if (i < msg->ChunkDeviceOffsets.size()) {
                ui64 deviceOffset = msg->ChunkDeviceOffsets[i];
                ChunkInfoMap[chunkIdx] = TChunkInfo(deviceOffset);

                LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
                    "DDiskActorImpl: Reserved chunk " << chunkIdx
                    << " with device offset " << deviceOffset);
            } else {
                LOG_WARN_S(ctx, NKikimrServices::BS_DDISK,
                    "DDiskActorImpl: No device offset for chunk " << chunkIdx);
            }
        }

        LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
            "DDiskActorImpl: Chunk reservation successful, chunk size: " << ChunkSize
            << " device path: " << (DevicePath ? DevicePath : "NONE"));

        // Transition to Ready state after first successful chunk allocation
        if (CurrentState == EDDiskState::Init && BlockDevice != nullptr) {
            LOG_INFO_S(ctx, NKikimrServices::BS_DDISK,
                "DDiskActorImpl: Transitioning to Ready state after receiving BlockDevice info");
            TransitionToReady(ctx);
        }

        // Broadcast updated chunk info to workers if they exist
        if (CurrentState == EDDiskState::Ready && !WorkerActors.empty()) {
            BroadcastChunkInfoUpdate(ctx);
        }
    } else {
        response->Record.SetErrorReason(msg->ErrorReason);
        LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
            "DDiskActorImpl: Chunk reservation failed: " << msg->ErrorReason);
    }

    // Send response to original sender
    ctx.Send(reservation.Sender, response.release(), 0, reservation.Cookie);

    // Clean up pending reservation
    PendingReservations.erase(it);
}

void TDDiskActorImpl::HandleChunkReadResult(
        const NPDisk::TEvChunkReadResult::TPtr& ev,
        const NActors::TActorContext& ctx) {
    Y_UNUSED(ev, ctx);
    Y_VERIFY_S(false, "Not expected HandleChunkReadResult");
}

void TDDiskActorImpl::HandleChunkWriteResult(
    const NPDisk::TEvChunkWriteResult::TPtr& ev,
    const NActors::TActorContext& ctx) {
    Y_UNUSED(ev, ctx);
    Y_VERIFY_S(false, "Not expected HandleChunkWriteResult");
}


void TDDiskActorImpl::InitializePDisk(const NActors::TActorContext& ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "DDiskActorImpl: Initializing PDisk connection for VDiskSlotId="
        << Config->BaseInfo.VDiskSlotId);

    // Send TEvYardInit to PDisk to establish ownership
    auto yardInitRequest = std::make_unique<NPDisk::TEvYardInit>(
        Config->BaseInfo.InitOwnerRound,
        SelfVDiskId,
        Config->BaseInfo.PDiskGuid,
        TActorId(),  // CutLogID - not used by DDisk
        TActorId(),  // WhiteboardProxyId - not used by DDisk
        Config->BaseInfo.VDiskSlotId
    );

    ctx.Send(Config->BaseInfo.PDiskActorID, yardInitRequest.release());
}

void TDDiskActorImpl::HandleYardInitResult(
    const NPDisk::TEvYardInitResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "DDiskActorImpl: HandleYardInitResult status=" << NKikimrProto::EReplyStatus_Name(msg->Status));

    if (msg->Status != NKikimrProto::OK) {
        LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
            "DDiskActorImpl: PDisk initialization failed: " << msg->ErrorReason);
        // TODO: should probably die or retry initialization
        return;
    }

    // Create PDisk context from the received parameters
    PDiskCtx = TPDiskCtx::Create(msg->PDiskParams, Config);
    ChunkSize = msg->PDiskParams->ChunkSize;  // Store chunk size from PDisk
    PDiskInitialized = true;

    LOG_INFO_S(ctx, NKikimrServices::BS_DDISK,
        "DDiskActorImpl: PDisk initialization complete. Owner=" << PDiskCtx->Dsk->Owner.Val
        << " OwnerRound=" << PDiskCtx->Dsk->OwnerRound
        << " ChunkSize=" << ChunkSize
        << " OwnedChunks=" << msg->OwnedChunks.size());

    // Track owned chunks for validation
    for (ui32 chunkIdx : msg->OwnedChunks) {
        KnownChunks.insert(chunkIdx);
        LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
            "DDiskActorImpl: Recovered owned chunk " << chunkIdx);
    }
}

void TDDiskActorImpl::HandlePing(
    const TEvBlobStorage::TEvDDiskPing::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    // Create Wilson span for tracing
    NWilson::TSpan span(TWilson::BlobStorage, std::move(ev->TraceId.Clone()), "DDisk.Ping");

    // Extract sender and cookie before releasing event to avoid accessing moved object
    TActorId sender = ev->Sender;
    ui64 cookie = ev->Cookie;

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "DDiskActorImpl: Received TEvDDiskPing from " << sender.ToString()
        << " cookie=" << cookie
        << " traceId=" << span.GetTraceId().GetHexTraceId());

    // Create and send response
    auto response = std::make_unique<TEvBlobStorage::TEvDDiskPingResponse>(NKikimrProto::OK);
    ctx.Send(sender, response.release(), 0, cookie);

    // Finish span with success
    span.EndOk();

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "DDiskActorImpl: TEvDDiskPing processed successfully for " << sender.ToString());
}

void TDDiskActorImpl::SendErrorResponse(const TPendingRequest& request, const TString& errorReason, const NActors::TActorContext& ctx)
{
    if (request.IsWrite) {
        auto response = std::make_unique<TEvBlobStorage::TEvDDiskWriteResponse>();
        response->Record.SetStatus(NKikimrProto::ERROR);
        response->Record.SetErrorReason(errorReason);
        response->Record.SetOffset(request.Offset);
        response->Record.SetSize(request.Size);
        ctx.Send(request.Sender, response.release(), 0, request.Cookie);
    } else {
        auto response = std::make_unique<TEvBlobStorage::TEvDDiskReadResponse>();
        response->Record.SetStatus(NKikimrProto::ERROR);
        response->Record.SetErrorReason(errorReason);
        response->Record.SetOffset(request.Offset);
        response->Record.SetSize(request.Size);
        // Store empty data as payload instead of in protobuf
        response->StorePayload(TRope(TString(request.Size, 0)));
        ctx.Send(request.Sender, response.release(), 0, request.Cookie);
    }
}

////////////////////////////////////////////////////////////////////////////////
// Worker pool management

void TDDiskActorImpl::TransitionToReady(const NActors::TActorContext& ctx)
{
    LOG_INFO_S(ctx, NKikimrServices::BS_DDISK,
        "DDiskActorImpl: State transition: Init -> Ready");

    CurrentState = EDDiskState::Ready;
    CreateWorkerPool(ctx);
}

void TDDiskActorImpl::CreateWorkerPool(const NActors::TActorContext& ctx)
{
    LOG_INFO_S(ctx, NKikimrServices::BS_DDISK,
        "DDiskActorImpl: Creating worker pool with " << WorkerCount << " workers");

    // Create worker config with thread-safe data
    TDDiskWorkerConfig config = CreateWorkerConfig();

    for (ui32 i = 0; i < WorkerCount; ++i) {
        auto worker = std::unique_ptr<NActors::IActor>(CreateDDiskWorkerActor(i, config));
        TActorId workerId = ctx.RegisterWithSameMailbox(worker.release());
        WorkerActors.push_back(workerId);

        LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
            "DDiskActorImpl: Created worker " << i << " with ActorId: " << workerId.ToString());
    }

    LOG_INFO_S(ctx, NKikimrServices::BS_DDISK,
        "DDiskActorImpl: Successfully created " << WorkerActors.size() << " worker actors");
}

TActorId TDDiskActorImpl::SelectNextWorker()
{
    if (WorkerActors.empty()) {
        return TActorId{};  // Return invalid ID if no workers
    }

    TActorId workerId = WorkerActors[NextWorkerIndex];
    NextWorkerIndex = (NextWorkerIndex + 1) % WorkerActors.size();
    return workerId;
}

TDDiskWorkerConfig TDDiskActorImpl::CreateWorkerConfig() const
{
    TDDiskWorkerConfig config;
    config.Mode = Mode;
    config.DevicePath = DevicePath;  // Pass device path for monitoring/debugging
    config.ChunkSize = ChunkSize;
    config.SelfVDiskId = SelfVDiskId;
    config.VDiskSlotId = Config->BaseInfo.VDiskSlotId;

    // Get shared file handle from PDisk's BlockDevice for DDisk workers
    if (BlockDevice) {
        config.SharedFileHandle = BlockDevice->GetFileHandle();
    } else {
        config.SharedFileHandle = nullptr;
    }

    // Copy chunk info map for thread safety
    config.ChunkInfoMap = ChunkInfoMap;

    return config;
}

void TDDiskActorImpl::BroadcastChunkInfoUpdate(const NActors::TActorContext& ctx)
{
    if (WorkerActors.empty()) {
        LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
            "DDiskActorImpl: No workers to broadcast chunk info update to");
        return;
    }

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "DDiskActorImpl: Broadcasting chunk info update to " << WorkerActors.size()
        << " workers with " << ChunkInfoMap.size() << " total chunks");

    // Send separate event to each worker (events are not copyable)
    for (const auto& workerId : WorkerActors) {
        auto chunkUpdate = std::make_unique<TEvDDiskChunkInfoUpdate>(ChunkInfoMap);
        ctx.Send(workerId, chunkUpdate.release());
    }

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "DDiskActorImpl: Chunk info broadcast completed");
}

}   // namespace NKikimr
