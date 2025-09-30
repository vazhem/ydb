#include "ddisk_worker_actor.h"
#include "ddisk_actor_mode_direct_completion.h"

#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TDDiskWorkerActor::TDDiskWorkerActor(ui32 workerId, const TDDiskWorkerConfig& config)
    : TActor(&TThis::StateWork)
    , WorkerId(workerId)
    , Config(config)
{
}

void TDDiskWorkerActor::HandleReadRequest(
    const TEvBlobStorage::TEvDDiskReadRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "[Worker" << WorkerId << "] Received DDisk read request from " << ev->Sender.ToString()
        << " chunkId=" << ev->Get()->Record.GetChunkId()
        << " offset=" << ev->Get()->Record.GetOffset()
        << " size=" << ev->Get()->Record.GetSize());

    ProcessDirectIORequest<TEvBlobStorage::TEvDDiskReadRequest>(ev, ctx);
}

void TDDiskWorkerActor::HandleWriteRequest(
    const TEvBlobStorage::TEvDDiskWriteRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "[Worker" << WorkerId << "] Received DDisk write request from " << ev->Sender.ToString()
        << " chunkId=" << ev->Get()->Record.GetChunkId()
        << " offset=" << ev->Get()->Record.GetOffset()
        << " size=" << ev->Get()->Record.GetSize());

    ProcessDirectIORequest<TEvBlobStorage::TEvDDiskWriteRequest>(ev, ctx);
}

void TDDiskWorkerActor::HandleChunkInfoUpdate(
    const TEvDDiskChunkInfoUpdate::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "[Worker" << WorkerId << "] Received chunk info update with "
        << msg->ChunkUpdates.size() << " chunk updates");

    // Update local chunk info map
    for (const auto& chunkUpdate : msg->ChunkUpdates) {
        Config.ChunkInfoMap[chunkUpdate.ChunkId] = TDDiskActorImpl::TChunkInfo(chunkUpdate.DeviceOffset);

        LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
            "[Worker" << WorkerId << "] Updated chunk " << chunkUpdate.ChunkId
            << " with device offset " << chunkUpdate.DeviceOffset);
    }

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "[Worker" << WorkerId << "] Chunk info update completed. Total chunks: "
        << Config.ChunkInfoMap.size());
}

template<typename TRequest>
void TDDiskWorkerActor::ProcessDirectIORequest(
    const typename TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    constexpr bool isRead = std::is_same_v<TRequest, TEvBlobStorage::TEvDDiskReadRequest>;
    const char* operationName = isRead ? "READ" : "WRITE";

    const auto* msg = ev->Get();
    const ui32 offset = msg->Record.GetOffset();
    const ui32 size = msg->Record.GetSize();
    const ui32 chunkId = msg->Record.GetChunkId();

    // Extract original request ID for traceability
    ui64 originalRequestId = ev->Cookie;
    TString traceIdStr = ev->TraceId.GetHexTraceId();

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
        "[Worker" << WorkerId << "] DDISK " << operationName << " REQUEST ENTRY: reqId=" << NextRequestCookie
        << " originalRequestId=" << originalRequestId
        << " traceId=" << traceIdStr
        << " offset=" << offset << " size=" << size << " chunkId=" << chunkId
        << " from=" << ev->Sender.ToString()
        << " vDiskId=" << Config.SelfVDiskId.ToString()
        << " vSlotId=" << Config.VDiskSlotId
        << " Mode=" << (ui32)Config.Mode
        << " BlockDevice=" << (void*)Config.BlockDevice);

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
    if (!Config.BlockDevice) {
        LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
            "[Worker" << WorkerId << "] Direct device I/O enabled but no block device available");
        sendErrorResponse("No block device available for direct I/O");
        return;
    }

    // Check if chunk is known and has device offset info
    auto chunkIt = Config.ChunkInfoMap.find(chunkId);
    if (chunkIt == Config.ChunkInfoMap.end()) {
        LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
            "[Worker" << WorkerId << "] No device offset info for chunk " << chunkId);
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
            "[Worker" << WorkerId << "] DIRECT WRITE ALIGNMENT: chunkId=" << chunkId
            << " originalOffset=" << offset << " originalSize=" << size
            << " deviceOffset=" << chunkIt->second.DeviceOffset
            << " actualDeviceOffset=" << actualDeviceOffset
            << " alignedDeviceOffset=" << alignedDeviceOffset
            << " offsetAdjustment=" << offsetAdjustment
            << " dataSize=" << data.size() << " alignedSize=" << alignedSize
            << " needsReadModifyWrite=" << needsReadModifyWrite);
    }

    if constexpr (isRead) {
        LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
            "[Worker" << WorkerId << "] DIRECT READ ALIGNMENT: chunkId=" << chunkId
            << " originalOffset=" << offset << " originalSize=" << size
            << " deviceOffset=" << chunkIt->second.DeviceOffset
            << " actualDeviceOffset=" << actualDeviceOffset
            << " alignedDeviceOffset=" << alignedDeviceOffset
            << " offsetAdjustment=" << offsetAdjustment
            << " adjustedSize=" << size + offsetAdjustment << " alignedSize=" << alignedSize);
    }

    // Create aligned buffer for I/O operations with proper alignment
    char* alignedData = static_cast<char*>(aligned_alloc(512, alignedSize));
    if (!alignedData) {
        LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
            "[Worker" << WorkerId << "] Failed to allocate aligned buffer for direct " << operationName << ", size=" << alignedSize);
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

            LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
                "[Worker" << WorkerId << "] READ-modify-write: copying data at offset " << offsetAdjustment
                << " within aligned buffer, dataSize=" << data.size());
        } else {
            // Simple case: just zero-initialize and copy data
            memset(alignedData, 0, alignedSize);
            memcpy(alignedData, data.data(), data.size());
        }
    }

    // Create completion handler that properly manages the buffer
    auto traceIdCopy = ev->TraceId.Clone();
    auto completion = new TDirectIOCompletion(ctx.SelfID, ev->Sender, ev->Cookie, originalRequestId, chunkId, offset, size, offsetAdjustment, isRead, alignedData, alignedSize, traceIdCopy);

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DEVICE,
        "[Worker" << WorkerId << "] DIRECT COMPLETION CREATED: VDiskSlotId=" << Config.VDiskSlotId
        << " action=" << (void*)completion << " isRead=" << isRead << " TraceId=" << traceIdStr
        << " RequestId=" << originalRequestId);

    // Safety checks to prevent crashes
    if (alignedSize == 0 || alignedSize > (1024 * 1024 * 1024)) {  // Max 1GB
        LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
            "[Worker" << WorkerId << "] INVALID ALIGNED SIZE: alignedSize=" << alignedSize << " - aborting operation");
        delete completion;
        free(alignedData);
        sendErrorResponse("Invalid aligned size");
        return;
    }

    if (alignedDeviceOffset > (1024ULL * 1024 * 1024 * 1024 * 10)) {  // Max 10TB
        LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
            "[Worker" << WorkerId << "] INVALID DEVICE OFFSET: alignedDeviceOffset=" << alignedDeviceOffset << " - aborting operation");
        delete completion;
        free(alignedData);
        sendErrorResponse("Invalid device offset");
        return;
    }

    // Perform the actual I/O operation
    if constexpr (isRead) {
        LOG_DEBUG_S(ctx, NKikimrServices::BS_DEVICE,
            "[Worker" << WorkerId << "] CALLING BlockDevice->PreadAsync: offset=" << alignedDeviceOffset
            << " size=" << alignedSize << " buffer=" << (void*)alignedData
            << " completion=" << (void*)completion);

        Config.BlockDevice->PreadAsync(alignedData, alignedSize, alignedDeviceOffset,
                                     completion, NPDisk::TReqId(), &traceIdCopy);
    } else {
        LOG_DEBUG_S(ctx, NKikimrServices::BS_DEVICE,
            "[Worker" << WorkerId << "] CALLING BlockDevice->PwriteAsync: offset=" << alignedDeviceOffset
            << " size=" << alignedSize << " buffer=" << (void*)alignedData
            << " completion=" << (void*)completion);

        Config.BlockDevice->PwriteAsync(alignedData, alignedSize, alignedDeviceOffset,
                                      completion, NPDisk::TReqId(), &traceIdCopy);
    }
}

////////////////////////////////////////////////////////////////////////////////

NActors::IActor* CreateDDiskWorkerActor(ui32 workerId, const TDDiskWorkerConfig& config) {
    return new TDDiskWorkerActor(workerId, config);
}

} // namespace NKikimr
