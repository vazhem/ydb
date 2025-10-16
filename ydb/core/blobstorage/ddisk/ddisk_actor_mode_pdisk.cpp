#include "ddisk_actor_mode_pdisk.h"
#include <ydb/library/wilson_ids/wilson.h>

namespace NKikimr {

TDDiskPDiskEventsActor::TDDiskPDiskEventsActor(
    TIntrusivePtr<TVDiskConfig> cfg,
    TIntrusivePtr<TBlobStorageGroupInfo> info,
    const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
    ui32 workerCount,
    ui32 chunksPerReservation)
    : TDDiskActorImpl(std::move(cfg), std::move(info), EDDiskMode::PDISK_EVENTS, counters, workerCount, chunksPerReservation)
{
}

void TDDiskPDiskEventsActor::HandleReadRequest(
    const TEvBlobStorage::TEvDDiskReadRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    // For PDISK_EVENTS mode, always handle requests directly in main actor, never forward to workers
    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "TDDiskPDiskEventsActor: Handling read request directly in main actor (no worker forwarding)");
    ProcessReadRequest(ev, ctx);
}

void TDDiskPDiskEventsActor::HandleWriteRequest(
    const TEvBlobStorage::TEvDDiskWriteRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    // For PDISK_EVENTS mode, always handle requests directly in main actor, never forward to workers
    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "TDDiskPDiskEventsActor: Handling write request directly in main actor (no worker forwarding)");
    ProcessWriteRequest(ev, ctx);
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

    NWilson::TSpan span(TWilson::BlobStorage, std::move(ev->TraceId.Clone()),
        "DDisk.Write.ProcessReadRequest");

    // Store pending request
    NWilson::TTraceId childTraceId = span.GetTraceId();
    PendingRequests[cookie] = TPendingRequest(offset, size, chunkId, ev->Sender, ev->Cookie, false,
        "", 0, false, std::move(span));

    // Send chunk read request to PDisk
    ui32 pdiskOffset = static_cast<ui32>(offset);
    auto pdiskRequest = std::make_unique<NPDisk::TEvChunkRead>(
        PDiskCtx->Dsk->Owner,
        PDiskCtx->Dsk->OwnerRound,
        chunkId,
        pdiskOffset,  // Use offset as provided by partition actor
        size,
        NPriRead::SyncLog,  // Priority class
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

    ctx.Send(PDiskCtx->PDiskId, pdiskRequest.release(), 0, 0, std::move(childTraceId));
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

    NWilson::TSpan span(TWilson::BlobStorage, std::move(ev->TraceId.Clone()),
        "DDisk.Write.ProcessWriteRequest");

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
    NWilson::TTraceId childTraceId = span.GetTraceId();
    PendingRequests[cookie] = TPendingRequest(offset, size, chunkId, ev->Sender, ev->Cookie, true, data,
        0, false, std::move(span));

    // Create write parts from data
    TRope rope(data);  // Create rope from the data string
    auto writeParts = MakeIntrusive<NPDisk::TEvChunkWrite::TRopeAlignedParts>(std::move(rope), size);

    // Send chunk write request to PDisk
    ui32 pdiskOffset = static_cast<ui32>(offset);
    auto pdiskRequest = std::make_unique<NPDisk::TEvChunkWrite>(
        PDiskCtx->Dsk->Owner,
        PDiskCtx->Dsk->OwnerRound,
        chunkId,
        pdiskOffset,  // Use offset as provided by partition actor
        writeParts,
        cookie,
        NPriWrite::SyncLog,  // Priority class
        true,  // DoFlush
        false   // IsSeqWrite
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

    ctx.Send(PDiskCtx->PDiskId, pdiskRequest.release(), 0, 0, std::move(childTraceId));
}


void TDDiskPDiskEventsActor::HandleChunkReadResult(
    const NPDisk::TEvChunkReadResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    void* cookie = msg->Cookie;

    auto it = PendingRequests.find(cookie);
    if (it == PendingRequests.end()) {
        LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
            "DDiskActorImpl: Received chunk read result for unknown request"
            << " cookie=" << 0
            << " pendingCount=" << PendingRequests.size());
        return;
    }

    TPendingRequest& request = it->second;

    // Log response data details for verification
    TString responseDataPrefix, responseDataSuffix;
    if (msg->Data.Size() >= 32 && msg->Data.IsReadable()) {
        TRcBuf responseDataBuf = msg->Data.ToString();
        TString responseDataStr = TString(responseDataBuf.GetData(), responseDataBuf.GetSize());
        for (int i = 0; i < std::min<int>(16, responseDataStr.size()); i++) {
            responseDataPrefix += TStringBuilder() << " " << (ui32)(ui8)responseDataStr[i];
        }
        if (responseDataStr.size() > 16) {
            for (int i = std::max<int>(16, static_cast<int>(responseDataStr.size()-16)); i < static_cast<int>(responseDataStr.size()); i++) {
                responseDataSuffix += TStringBuilder() << " " << (ui32)(ui8)responseDataStr[i];
            }
        }
    }

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "📥 PDISK → DDISK READ RESULT: reqId=" << 0
        << " status=" << NKikimrProto::EReplyStatus_Name(msg->Status)
        << " chunkId=" << msg->ChunkIdx << " originalOffset=" << request.Offset
        << " requestedSize=" << request.Size << " receivedSize=" << msg->Data.Size()
        << " vDiskId=" << SelfVDiskId.ToString()
        << " vSlotId=" << Config->BaseInfo.VDiskSlotId
        << " pdisk=" << PDiskCtx->PDiskId.ToString()
        << " dataPrefix[" << responseDataPrefix << " ]"
        << " dataSuffix[" << responseDataSuffix << " ]");

    // Create response
    auto response = std::make_unique<TEvBlobStorage::TEvDDiskReadResponse>();
    response->Record.SetStatus(msg->Status);
    response->Record.SetErrorReason(msg->ErrorReason);
    response->Record.SetOffset(request.Offset);
    response->Record.SetSize(request.Size);

    if (msg->Status == NKikimrProto::OK) {
        // Extract data from PDisk response
        TString data(request.Size, 0);  // Initialize with zeros

        // Check if we have any data and if it's readable
        if (msg->Data.Size() > 0) {
            // Check if the entire buffer is readable (no gaps)
            if (msg->Data.IsReadable()) {
                // Data is clean, extract what we can
                TRcBuf responseDataBuf = msg->Data.ToString();
                size_t copySize = Min(static_cast<size_t>(responseDataBuf.GetSize()), static_cast<size_t>(request.Size));
                memcpy(const_cast<char*>(data.data()), responseDataBuf.GetData(), copySize);
            } else {
                // Data has gaps - try to extract readable portions
                // For now, we'll return zeros for corrupt data
                LOG_WARN_S(ctx, NKikimrServices::BS_DDISK,
                    "DDiskActorImpl: Received data with gaps from PDisk, chunk=" << msg->ChunkIdx
                    << " returning zeros for request offset=" << request.Offset
                    << " size=" << request.Size);
            }
        }

        response->Record.SetData(std::move(data));
    } else {
        // Return zeros on error
        response->Record.SetData(TString(request.Size, 0));
    }

    // Send response back
    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "🚀 SENDING DDISK READ RESPONSE: reqId=" << 0
        << " to=" << request.Sender.ToString() << " cookie=" << reinterpret_cast<uintptr_t>(request.Cookie)
        << " status=" << NKikimrProto::EReplyStatus_Name(response->Record.GetStatus())
        << " dataSize=" << response->Record.GetData().size()
        << " chunkId=" << response->Record.GetChunkId()
        << " vDiskId=" << SelfVDiskId.ToString());

    ctx.Send(request.Sender, response.release(), 0, request.Cookie);

    // End span with appropriate status
    if (request.Span) {
        if (msg->Status == NKikimrProto::OK) {
            request.Span.EndOk();
        } else {
            request.Span.EndError(msg->ErrorReason);
        }
    }

    // Clean up pending request
    PendingRequests.erase(it);
}

void TDDiskPDiskEventsActor::HandleChunkWriteResult(
    const NPDisk::TEvChunkWriteResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    void* cookie = msg->Cookie;

    auto it = PendingRequests.find(cookie);
    if (it == PendingRequests.end()) {
        LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
            "DDiskActorImpl: Received chunk write result for unknown request"
            << " cookie=" << 0
            << " pendingCount=" << PendingRequests.size());
        return;
    }

    TPendingRequest& request = it->second;

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "📥 PDISK → DDISK WRITE RESULT: reqId=" << 0
        << " status=" << NKikimrProto::EReplyStatus_Name(msg->Status)
        << " chunkId=" << msg->ChunkIdx << " originalOffset=" << request.Offset
        << " requestedSize=" << request.Size
        << " vDiskId=" << SelfVDiskId.ToString()
        << " vSlotId=" << Config->BaseInfo.VDiskSlotId
        << " pdisk=" << PDiskCtx->PDiskId.ToString()
        << " writtenDataSize=" << request.WriteData.size());

    // Create response
    auto response = std::make_unique<TEvBlobStorage::TEvDDiskWriteResponse>();
    response->Record.SetStatus(msg->Status);
    response->Record.SetErrorReason(msg->ErrorReason);
    response->Record.SetOffset(request.Offset);
    response->Record.SetSize(request.Size);
    response->Record.SetChunkId(request.ChunkId);

    // Send response back
    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "🚀 SENDING DDISK RESPONSE: reqId=" << 0
        << " to=" << request.Sender.ToString() << " cookie=" << reinterpret_cast<uintptr_t>(request.Cookie)
        << " status=" << NKikimrProto::EReplyStatus_Name(response->Record.GetStatus())
        << " vDiskId=" << SelfVDiskId.ToString());

    ctx.Send(request.Sender, response.release(), 0, request.Cookie);

    // End span with appropriate status
    if (request.Span) {
        if (msg->Status == NKikimrProto::OK) {
            request.Span.EndOk();
        } else {
            request.Span.EndError(msg->ErrorReason);
        }
    }

    // Clean up pending request
    PendingRequests.erase(it);
}

}   // namespace NKikimr
