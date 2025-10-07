#include "ddisk_actor_mode_memory.h"
#include <ydb/library/wilson_ids/wilson.h>

namespace NKikimr {

TDDiskMemoryActor::TDDiskMemoryActor(
    TIntrusivePtr<TVDiskConfig> cfg,
    TIntrusivePtr<TBlobStorageGroupInfo> info,
    const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
    ui32 workerCount,
    ui32 chunksPerReservation)
    : TDDiskActorImpl(std::move(cfg), std::move(info), EDDiskMode::MEMORY, counters, workerCount, chunksPerReservation)
{
}

void TDDiskMemoryActor::ProcessReadRequest(
    const TEvBlobStorage::TEvDDiskReadRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    NWilson::TSpan span(TWilson::BlobStorage, std::move(ev->TraceId.Clone()), "DDisk.Read");

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
        << " Mode=MEMORY");

    // Check if chunk exists in memory
    auto chunkIt = InMemoryChunks.find(chunkId);
    if (chunkIt != InMemoryChunks.end()) {
        // Read data from in-memory chunk
        TString data = chunkIt->second.ReadData(offset, size);
        auto response = std::make_unique<TEvBlobStorage::TEvDDiskReadResponse>();
        response->Record.SetOffset(offset);
        response->Record.SetSize(size);
        response->Record.SetChunkId(chunkId);
        response->Record.SetData(data);
        response->Record.SetStatus(NKikimrProto::OK);

        // Log read data for debugging
        TString dataLogPrefix;
        if (data.size() >= 16) {
            for (int i = 0; i < std::min<int>(16, data.size()); i++) {
                dataLogPrefix += TStringBuilder() << " " << (ui32)(ui8)data[i];
            }
        }

        LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
            "📥 IN-MEMORY READ RESULT: reqId=" << NextRequestCookie
            << " chunkId=" << chunkId << " offset=" << offset << " size=" << size
            << " status=OK dataPrefix[" << dataLogPrefix << " ]");

        ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
    } else {
        // Chunk not found, return zeros (uninitialized data)
        auto response = std::make_unique<TEvBlobStorage::TEvDDiskReadResponse>();
        response->Record.SetOffset(offset);
        response->Record.SetSize(size);
        response->Record.SetChunkId(chunkId);
        response->Record.SetData(TString(size, '\0'));
        response->Record.SetStatus(NKikimrProto::OK);

        LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
            "📥 IN-MEMORY READ RESULT: reqId=" << NextRequestCookie
            << " chunkId=" << chunkId << " offset=" << offset << " size=" << size
            << " status=OK_UNINITIALIZED_CHUNK (returning zeros)");

        ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
    }

    NextRequestCookie++;

    span.EndOk();
}

void TDDiskMemoryActor::ProcessWriteRequest(
    const TEvBlobStorage::TEvDDiskWriteRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    NWilson::TSpan span(TWilson::BlobStorage, std::move(ev->TraceId.Clone()), "DDisk.Write");

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
        << " Mode=MEMORY"
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

    // Ensure chunk exists in memory
    auto& chunk = InMemoryChunks[chunkId];

    // Write data to in-memory chunk
    chunk.WriteData(offset, data);

    // Create immediate success response
    auto response = std::make_unique<TEvBlobStorage::TEvDDiskWriteResponse>();
    response->Record.SetOffset(offset);
    response->Record.SetSize(size);
    response->Record.SetChunkId(chunkId);
    response->Record.SetStatus(NKikimrProto::OK);

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "📥 IN-MEMORY WRITE RESULT: reqId=" << NextRequestCookie
        << " chunkId=" << chunkId << " offset=" << offset << " size=" << size
        << " status=OK");

    ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
    NextRequestCookie++;

    span.EndOk();
}

void TDDiskMemoryActor::HandleReadRequest(
    const TEvBlobStorage::TEvDDiskReadRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    // For memory mode, always handle requests directly in main actor, never forward to workers
    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "TDDiskMemoryActor: Handling read request directly in main actor (no worker forwarding)");
    ProcessReadRequest(ev, ctx);
}

void TDDiskMemoryActor::HandleWriteRequest(
    const TEvBlobStorage::TEvDDiskWriteRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    // For memory mode, always handle requests directly in main actor, never forward to workers
    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "TDDiskMemoryActor: Handling write request directly in main actor (no worker forwarding)");
    ProcessWriteRequest(ev, ctx);
}

}   // namespace NKikimr
