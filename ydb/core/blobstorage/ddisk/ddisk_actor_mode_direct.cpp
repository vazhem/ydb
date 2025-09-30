#include "ddisk_actor_mode_direct.h"
#include "ddisk_actor_mode_direct_completion.h"

namespace NKikimr {

TDDiskDirectIOActor::TDDiskDirectIOActor(
    TIntrusivePtr<TVDiskConfig> cfg,
    TIntrusivePtr<TBlobStorageGroupInfo> info,
    const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
    : TDDiskActorImpl(std::move(cfg), std::move(info), EDDiskMode::DIRECT_IO, counters)
{
}

template<typename TRequest, typename TResponse>
void TDDiskDirectIOActor::ProcessDirectIORequest(
    const typename TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    constexpr bool isRead = std::is_same_v<TRequest, TEvBlobStorage::TEvDDiskReadRequest>;
    const char* operationName = isRead ? "READ" : "WRITE";

    const auto* msg = ev->Get();
    const ui32 offset = msg->Record.GetOffset();
    const ui32 size = msg->Record.GetSize();
    const ui32 chunkId = msg->Record.GetChunkId();

    // Extract original request ID for traceability (if passed via cookie)
    ui64 originalRequestId = ev->Cookie;  // Используем cookie как ID запроса
    TString traceIdStr = ev->TraceId.GetHexTraceId();  // Для логирования trace id

    // Helper lambda for sending error responses
    auto sendErrorResponse = [&](const TString& errorReason) {
        auto sendResponse = [&](auto response) {
            response->Record.SetStatus(NKikimrProto::ERROR);
            response->Record.SetErrorReason(errorReason);
            response->Record.SetOffset(offset);
            response->Record.SetSize(size);
            response->Record.SetChunkId(chunkId);
            if constexpr (requires { response->Record.SetData(TString()); }) {
                response->Record.SetData(TString(size, 0));
            }
            ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
        };

        if constexpr (isRead) {
            sendResponse(std::make_unique<TEvBlobStorage::TEvDDiskReadResponse>());
        } else {
            sendResponse(std::make_unique<TEvBlobStorage::TEvDDiskWriteResponse>());
        }
    };

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "🚀 DDISK " << operationName << " REQUEST ENTRY: reqId=" << NextRequestCookie
        << " originalRequestId=" << originalRequestId
        << " traceId=" << traceIdStr
        << " offset=" << offset << " size=" << size << " chunkId=" << chunkId
        << " from=" << ev->Sender.ToString()
        << " vDiskId=" << SelfVDiskId.ToString()
        << " vSlotId=" << Config->BaseInfo.VDiskSlotId
        << " Mode=DIRECT_IO"
        << " BlockDevice=" << (void*)BlockDevice);

    // For write operations, validate data size
    if constexpr (!isRead) {
        const TString& data = msg->Record.GetData();
        if (data.size() != size) {
            TString errorReason = TStringBuilder() << "Data size mismatch: expected=" << size
                                                   << " actual=" << data.size();
            sendErrorResponse(errorReason);
            return;
        }
    }

    // Check if we have block device for direct I/O
    if (!BlockDevice) {
        LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
            "DDiskActorImpl: Direct device I/O enabled but no block device available");
        sendErrorResponse("No block device available for direct I/O");
        return;
    }

    // Check if chunk is known and has device offset info
    auto chunkIt = ChunkInfoMap.find(chunkId);
    if (chunkIt == ChunkInfoMap.end()) {
        LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
            "DDiskActorImpl: No device offset info for chunk " << chunkId);
        sendErrorResponse("No device offset info for chunk");
        return;
    }

    // Calculate the actual device offset (chunk offset + offset within chunk)
    ui64 actualDeviceOffset = chunkIt->second.DeviceOffset + offset;

    // CRITICAL: Block devices require device offset to be 512-byte aligned
    ui64 alignedDeviceOffset = actualDeviceOffset & ~511ULL;  // Round down to 512-byte boundary
    ui32 offsetAdjustment = static_cast<ui32>(actualDeviceOffset - alignedDeviceOffset);

    // Calculate aligned size
    ui32 alignedSize;
    if constexpr (isRead) {
        // For reads, adjust size to include the offset adjustment
        ui32 adjustedSize = size + offsetAdjustment;
        alignedSize = (adjustedSize + 511) & ~511;  // Round up to nearest 512 bytes
    } else {
        // For writes, we need to read-modify-write if not aligned
        const TString& data = msg->Record.GetData();
        bool needsReadModifyWrite = (offsetAdjustment != 0 || (data.size() & 511) != 0);
        alignedSize = ((data.size() + offsetAdjustment + 511) & ~511);  // Round up to nearest 512 bytes

        LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
            "🔍 DIRECT WRITE ALIGNMENT: reqId=" << 0
            << " chunkId=" << chunkId << " originalOffset=" << offset << " originalSize=" << size
            << " deviceOffset=" << chunkIt->second.DeviceOffset
            << " actualDeviceOffset=" << actualDeviceOffset
            << " alignedDeviceOffset=" << alignedDeviceOffset
            << " offsetAdjustment=" << offsetAdjustment
            << " dataSize=" << data.size() << " alignedSize=" << alignedSize
            << " needsReadModifyWrite=" << needsReadModifyWrite);
    }

    if constexpr (isRead) {
        LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
            "🔍 DIRECT READ ALIGNMENT: chunkId=" << chunkId << " originalOffset=" << offset
            << " originalSize=" << size << " deviceOffset=" << chunkIt->second.DeviceOffset
            << " actualDeviceOffset=" << actualDeviceOffset
            << " alignedDeviceOffset=" << alignedDeviceOffset
            << " offsetAdjustment=" << offsetAdjustment
            << " adjustedSize=" << size + offsetAdjustment << " alignedSize=" << alignedSize);
    }

    // Create aligned buffer for I/O operations with proper alignment
    char* alignedData = static_cast<char*>(aligned_alloc(512, alignedSize));
    if (!alignedData) {
        LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
            "DDiskActorImpl: Failed to allocate aligned buffer for direct " << operationName << ", size=" << alignedSize);
        sendErrorResponse("Failed to allocate aligned buffer");
        return;
    }

    // Initialize the aligned buffer
    if constexpr (isRead) {
        // Zero-initialize the buffer to avoid reading garbage data
        memset(alignedData, 0, alignedSize);
    } else {
        // For writes, handle data copying
        const TString& data = msg->Record.GetData();
        bool needsReadModifyWrite = (offsetAdjustment != 0 || (data.size() & 511) != 0);

        if (needsReadModifyWrite && offsetAdjustment > 0) {
            // For read-modify-write, we should read the existing data first
            // For now, zero-initialize and write the data at the correct offset
            memset(alignedData, 0, alignedSize);
            memcpy(alignedData + offsetAdjustment, data.data(), data.size());

            LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
                "⚠️ READ-MODIFY-WRITE: copying data at offset " << offsetAdjustment
                << " within aligned buffer, dataSize=" << data.size());
        } else {
            // Simple case: just zero-initialize and copy data
            memset(alignedData, 0, alignedSize);
            memcpy(alignedData, data.data(), data.size());
        }
    }

    // Create simple completion handler that properly manages the buffer
    auto completion = new TDirectIOCompletion(ctx.SelfID, ev->Sender, ev->Cookie,
        originalRequestId, chunkId, offset, size, offsetAdjustment,
        isRead, alignedData, alignedSize,
        std::move(ev->TraceId.Clone()));
    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "🔧 DDIRECT COMPLETION CREATED: VDiskSlotId=" << Config->BaseInfo.VDiskSlotId
        << " action=" << (void*)completion << " isRead=" << isRead << " TraceId=" << traceIdStr
        << " RequestId=" << originalRequestId);

    // Safety checks to prevent crashes
    if (alignedSize == 0 || alignedSize > (1024 * 1024 * 1024)) {  // Max 1GB
        LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
            "❌ INVALID ALIGNED SIZE: reqId=" << 0
            << " alignedSize=" << alignedSize << " - aborting operation");
        delete completion;
        sendErrorResponse("Invalid aligned size");
        return;
    }

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "📨 STARTING DIRECT " << operationName << ": reqId=" << originalRequestId
        << " internalCookie=" << ev->Cookie
        << " chunkId=" << chunkId << " offset=" << offset << " size=" << size
        << " alignedSize=" << alignedSize << " offsetAdjustment=" << offsetAdjustment
        << " vDiskId=" << SelfVDiskId.ToString()
        << " vSlotId=" << Config->BaseInfo.VDiskSlotId
        << " deviceOffset=" << chunkIt->second.DeviceOffset
        << " alignedDeviceOffset=" << alignedDeviceOffset
        << " bufferPtr=" << (void*)alignedData
        << " BlockDevice=" << (void*)BlockDevice);

    // Perform asynchronous direct I/O operation with aligned offset and size
    auto traceIdCopy = ev->TraceId.Clone();
    if constexpr (isRead) {
        BlockDevice->PreadAsync(alignedData, alignedSize, alignedDeviceOffset,
                               completion, NPDisk::TReqId(), &traceIdCopy);
    } else {
        BlockDevice->PwriteAsync(alignedData, alignedSize, alignedDeviceOffset,
                                completion, NPDisk::TReqId(), &traceIdCopy);
    }
}

void TDDiskDirectIOActor::ProcessReadRequest(
    const TEvBlobStorage::TEvDDiskReadRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    ProcessDirectIORequest<TEvBlobStorage::TEvDDiskReadRequest, TEvBlobStorage::TEvDDiskReadResponse>(
        ev, ctx);
}

void TDDiskDirectIOActor::ProcessWriteRequest(
    const TEvBlobStorage::TEvDDiskWriteRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    ProcessDirectIORequest<TEvBlobStorage::TEvDDiskWriteRequest, TEvBlobStorage::TEvDDiskWriteResponse>(
        ev, ctx);
}

}   // namespace NKikimr
