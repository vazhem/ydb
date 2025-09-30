#include "ddisk_actor_impl.h"

#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>

namespace NKikimr {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TDDiskActorImpl::Bootstrap(const NActors::TActorContext& ctx)
{
    SelfVDiskId = GInfo->GetVDiskId(Config->BaseInfo.VDiskIdShort);

    LOG_INFO_S(ctx, NKikimrServices::BS_DDISK,
        "DDiskActorImpl: Bootstrap starting for VDiskId=" << SelfVDiskId.ToString()
        << " VDiskSlotId=" << Config->BaseInfo.VDiskSlotId);

    // Initialize PDisk connection
    InitializePDisk(ctx);

    Become(&TThis::StateWork);
}

void TDDiskActorImpl::HandleReadRequest(
    const TEvBlobStorage::TEvDDiskReadRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const ui64 offset = msg->Record.GetOffset();
    const ui32 size = msg->Record.GetSize();
    const ui32 chunkId = msg->Record.GetChunkId();

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "DDiskActorImpl: HandleReadRequest offset=" << offset
        << " size=" << size << " chunkId=" << chunkId
        << " from=" << ev->Sender.ToString()
        << " cookie=" << reinterpret_cast<uintptr_t>(reinterpret_cast<void*>(NextRequestCookie)));

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
    auto pdiskRequest = std::make_unique<NPDisk::TEvChunkRead>(
        PDiskCtx->Dsk->Owner,
        PDiskCtx->Dsk->OwnerRound,
        chunkId,
        static_cast<ui32>(offset),  // Use offset as provided by partition actor
        size,
        NPriRead::HullLow,  // Priority class
        cookie
    );

    ctx.Send(PDiskCtx->PDiskId, pdiskRequest.release());
}

void TDDiskActorImpl::HandleWriteRequest(
    const TEvBlobStorage::TEvDDiskWriteRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const ui32 offset = msg->Record.GetOffset();  // Use ui32 for chunk-relative offset
    const ui32 size = msg->Record.GetSize();
    const ui32 chunkId = msg->Record.GetChunkId();
    const TString& data = msg->Record.GetData();

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "DDiskActorImpl: HandleWriteRequest offset=" << offset
        << " size=" << size << " chunkId=" << chunkId
        << " from=" << ev->Sender.ToString()
        << " cookie=" << reinterpret_cast<uintptr_t>(reinterpret_cast<void*>(NextRequestCookie)));

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
    auto writeParts = MakeIntrusive<NPDisk::TEvChunkWrite::TStrokaBackedUpParts>(const_cast<TString&>(data));

    // Send chunk write request to PDisk
    auto pdiskRequest = std::make_unique<NPDisk::TEvChunkWrite>(
        PDiskCtx->Dsk->Owner,
        PDiskCtx->Dsk->OwnerRound,
        chunkId,
        static_cast<ui32>(offset),  // Use offset as provided by partition actor
        writeParts,
        cookie,
        NPriWrite::HullHugeAsyncBlob,  // Priority class
        true,  // DoFlush
        true   // IsSeqWrite
    );

    ctx.Send(PDiskCtx->PDiskId, pdiskRequest.release());
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

        // Add reserved chunks to known chunks for validation
        for (ui32 chunkIdx : msg->ChunkIds) {
            KnownChunks.insert(chunkIdx);
            response->Record.AddChunkIds(chunkIdx);

            LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
                "DDiskActorImpl: Reserved chunk " << chunkIdx);
        }

        LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
            "DDiskActorImpl: Chunk reservation successful, chunk size: " << ChunkSize);
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
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    void* cookie = msg->Cookie;

    auto it = PendingRequests.find(cookie);
    if (it == PendingRequests.end()) {
        LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
            "DDiskActorImpl: Received chunk read result for unknown request"
            << " cookie=" << reinterpret_cast<uintptr_t>(cookie)
            << " pendingCount=" << PendingRequests.size());
        return;
    }

    const TPendingRequest& request = it->second;

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "DDiskActorImpl: HandleChunkReadResult status=" << NKikimrProto::EReplyStatus_Name(msg->Status)
        << " chunk=" << msg->ChunkIdx << " offset=" << request.Offset);

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
                TRcBuf rcBuf = msg->Data.ToString();
                size_t copySize = Min(static_cast<size_t>(rcBuf.size()), static_cast<size_t>(request.Size));
                memcpy(const_cast<char*>(data.data()), rcBuf.data(), copySize);
            } else {
                // Data has gaps - try to extract readable portions
                // For now, we'll return zeros for corrupt data
                // In a production system, you might want to:
                // 1. Try to read individual readable ranges
                // 2. Return partial data with gap information
                // 3. Trigger data recovery/repair

                LOG_WARN_S(ctx, NKikimrServices::BS_DDISK,
                    "DDiskActorImpl: Received data with gaps from PDisk, chunk=" << msg->ChunkIdx
                    << " returning zeros for request offset=" << request.Offset
                    << " size=" << request.Size);

                // data is already initialized with zeros above
            }
        }

        response->Record.SetData(std::move(data));
    } else {
        // Return zeros on error
        response->Record.SetData(TString(request.Size, 0));
    }

    // Send response back
    ctx.Send(request.Sender, response.release(), 0, request.Cookie);

    // Clean up pending request
    PendingRequests.erase(it);
}

void TDDiskActorImpl::HandleChunkWriteResult(
    const NPDisk::TEvChunkWriteResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    void* cookie = msg->Cookie;

    auto it = PendingRequests.find(cookie);
    if (it == PendingRequests.end()) {
        LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
            "DDiskActorImpl: Received chunk write result for unknown request"
            << " cookie=" << reinterpret_cast<uintptr_t>(cookie)
            << " pendingCount=" << PendingRequests.size());
        return;
    }

    const TPendingRequest& request = it->second;

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "DDiskActorImpl: HandleChunkWriteResult status=" << NKikimrProto::EReplyStatus_Name(msg->Status)
        << " chunk=" << msg->ChunkIdx << " offset=" << request.Offset);

    // Create response
    auto response = std::make_unique<TEvBlobStorage::TEvDDiskWriteResponse>();
    response->Record.SetStatus(msg->Status);
    response->Record.SetErrorReason(msg->ErrorReason);
    response->Record.SetOffset(request.Offset);
    response->Record.SetSize(request.Size);
    response->Record.SetChunkId(request.ChunkId);

    // Send response back
    ctx.Send(request.Sender, response.release(), 0, request.Cookie);

    // Clean up pending request
    PendingRequests.erase(it);
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
        response->Record.SetData(TString(request.Size, 0));
        ctx.Send(request.Sender, response.release(), 0, request.Cookie);
    }
}

}   // namespace NKikimr
