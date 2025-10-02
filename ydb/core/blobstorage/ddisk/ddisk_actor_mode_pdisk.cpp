#include "ddisk_actor_mode_pdisk.h"

namespace NKikimr {

TDDiskPDiskEventsActor::TDDiskPDiskEventsActor(
    TIntrusivePtr<TVDiskConfig> cfg,
    TIntrusivePtr<TBlobStorageGroupInfo> info,
    const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
    : TDDiskActorImpl(std::move(cfg), std::move(info), EDDiskMode::PDISK_EVENTS, counters)
{
}

void TDDiskPDiskEventsActor::ProcessReadRequest(
    const TEvBlobStorage::TEvDDiskReadRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const ui64 offset = msg->Record.GetOffset();
    const ui32 size = msg->Record.GetSize();
    const ui32 chunkId = msg->Record.GetChunkId();

    // Extract original request ID for traceability (if passed via cookie)
    ui64 originalRequestId = ev->Cookie;  // Используем cookie как ID запроса
    TString traceIdStr = ev->TraceId.GetHexTraceId();  // Для логирования trace id

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "🚀 DDISK READ REQUEST ENTRY: reqId=" << NextRequestCookie
        << " originalRequestId=" << originalRequestId
        << " traceId=" << traceIdStr
        << " offset=" << offset << " size=" << size << " chunkId=" << chunkId
        << " from=" << ev->Sender.ToString()
        << " vDiskId=" << SelfVDiskId.ToString()
        << " vSlotId=" << Config->BaseInfo.VDiskSlotId
        << " Mode=PDISK_EVENTS");

    // ORIGINAL PDisk PATH
    if (!PDiskInitialized) {
        // PDisk not yet initialized, return error
        auto response = std::make_unique<TEvBlobStorage::TEvDDiskReadResponse>();
        response->Record.SetStatus(NKikimrProto::ERROR);
        response->Record.SetErrorReason("PDisk not initialized");
        response->Record.SetOffset(offset);
        response->Record.SetSize(size);
        response->Record.SetChunkId(chunkId);
        response->Record.SetData(TString(size, 0));
        ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
        return;
    }

    // Quick validation of chunk ID
    if (KnownChunks.find(chunkId) == KnownChunks.end()) {
        auto response = std::make_unique<TEvBlobStorage::TEvDDiskReadResponse>();
        response->Record.SetStatus(NKikimrProto::ERROR);
        response->Record.SetErrorReason("Unknown chunk ID");
        response->Record.SetOffset(offset);
        response->Record.SetSize(size);
        response->Record.SetChunkId(chunkId);
        response->Record.SetData(TString(size, 0));
        ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
        return;
    }

    // Create a unique cookie for this request using incrementing counter
    void* cookie = reinterpret_cast<void*>(static_cast<uintptr_t>(NextRequestCookie++));

    // Store pending request
    PendingRequests[cookie] = TPendingRequest(offset, size, chunkId, ev->Sender, ev->Cookie, false);

    // Send chunk read request to PDisk
    ui32 pdiskOffset = static_cast<ui32>(offset);
    auto pdiskRequest = std::make_unique<NPDisk::TEvChunkRead>(
        PDiskCtx->Dsk->Owner,
        PDiskCtx->Dsk->OwnerRound,
        chunkId,
        pdiskOffset,  // Use offset as provided by partition actor
        size,
        NPriRead::HullLow,  // Priority class
        cookie
    );

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "📨 DDISK → PDISK READ: reqId=" << 0
        << " chunkId=" << chunkId << " pdiskOffset=" << pdiskOffset
        << " originalOffset=" << offset << " size=" << size
        << " vDiskId=" << SelfVDiskId.ToString()
        << " vSlotId=" << Config->BaseInfo.VDiskSlotId
        << " → pdisk=" << PDiskCtx->PDiskId.ToString()
        << " owner=" << PDiskCtx->Dsk->Owner.Val
        << " ownerRound=" << PDiskCtx->Dsk->OwnerRound);

    ctx.Send(PDiskCtx->PDiskId, pdiskRequest.release());
}

void TDDiskPDiskEventsActor::ProcessWriteRequest(
    const TEvBlobStorage::TEvDDiskWriteRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const ui32 offset = msg->Record.GetOffset();  // Use ui32 for chunk-relative offset
    const ui32 size = msg->Record.GetSize();
    const ui32 chunkId = msg->Record.GetChunkId();
    const TString& data = msg->Record.GetData();

    // Extract original request ID for traceability (if passed via cookie)
    ui64 originalRequestId = ev->Cookie;  // Используем cookie как ID запроса
    TString traceIdStr = ev->TraceId.GetHexTraceId();  // Для логирования trace id

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "🚀 DDISK WRITE REQUEST ENTRY: reqId=" << NextRequestCookie
        << " originalRequestId=" << originalRequestId
        << " traceId=" << traceIdStr
        << " offset=" << offset << " size=" << size << " chunkId=" << chunkId
        << " from=" << ev->Sender.ToString()
        << " vDiskId=" << SelfVDiskId.ToString()
        << " vSlotId=" << Config->BaseInfo.VDiskSlotId
        << " Mode=PDISK_EVENTS"
        << " dataSize=" << data.size());

    // Validate data size
    if (data.size() != size) {
        TString errorReason = TStringBuilder() << "Data size mismatch: expected=" << size
                                               << " actual=" << data.size();
        auto response = std::make_unique<TEvBlobStorage::TEvDDiskWriteResponse>();
        response->Record.SetStatus(NKikimrProto::ERROR);
        response->Record.SetErrorReason(errorReason);
        response->Record.SetOffset(offset);
        response->Record.SetSize(size);
        response->Record.SetChunkId(chunkId);
        ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
        return;
    }

    // ORIGINAL PDisk PATH
    if (!PDiskInitialized) {
        // PDisk not yet initialized, return error
        auto response = std::make_unique<TEvBlobStorage::TEvDDiskWriteResponse>();
        response->Record.SetStatus(NKikimrProto::ERROR);
        response->Record.SetErrorReason("PDisk not initialized");
        response->Record.SetOffset(offset);
        response->Record.SetSize(size);
        response->Record.SetChunkId(chunkId);
        ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
        return;
    }

    // Quick validation of chunk ID
    if (KnownChunks.find(chunkId) == KnownChunks.end()) {
        auto response = std::make_unique<TEvBlobStorage::TEvDDiskWriteResponse>();
        response->Record.SetStatus(NKikimrProto::ERROR);
        response->Record.SetErrorReason("Unknown chunk ID");
        response->Record.SetOffset(offset);
        response->Record.SetSize(size);
        response->Record.SetChunkId(chunkId);
        ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
        return;
    }

    // Create a unique cookie for this request using incrementing counter
    void* cookie = reinterpret_cast<void*>(static_cast<uintptr_t>(NextRequestCookie++));

    // Store pending request
    PendingRequests[cookie] = TPendingRequest(offset, size, chunkId, ev->Sender, ev->Cookie, true, data);

    // Create write parts from data
    // TODO: fix
    TRope rope;
    size_t rope_size;
    auto writeParts = MakeIntrusive<NPDisk::TEvChunkWrite::TRopeAlignedParts>(std::move(rope), rope_size);

    // Send chunk write request to PDisk
    ui32 pdiskOffset = static_cast<ui32>(offset);
    auto pdiskRequest = std::make_unique<NPDisk::TEvChunkWrite>(
        PDiskCtx->Dsk->Owner,
        PDiskCtx->Dsk->OwnerRound,
        chunkId,
        pdiskOffset,  // Use offset as provided by partition actor
        writeParts,
        cookie,
        NPriWrite::HullHugeAsyncBlob,  // Priority class
        true,  // DoFlush
        true   // IsSeqWrite
    );

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "📨 DDISK → PDISK WRITE: reqId=" << 0
        << " chunkId=" << chunkId << " pdiskOffset=" << pdiskOffset
        << " originalOffset=" << offset << " size=" << size
        << " vDiskId=" << SelfVDiskId.ToString()
        << " vSlotId=" << Config->BaseInfo.VDiskSlotId
        << " → pdisk=" << PDiskCtx->PDiskId.ToString()
        << " owner=" << PDiskCtx->Dsk->Owner.Val
        << " ownerRound=" << PDiskCtx->Dsk->OwnerRound
        << " dataSize=" << data.size());

    ctx.Send(PDiskCtx->PDiskId, pdiskRequest.release());
}

}   // namespace NKikimr
