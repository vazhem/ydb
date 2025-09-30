#include "ddisk_actor_impl.h"

#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/library/pdisk_io/buffers.h>

namespace NKikimr {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TDDiskActorImpl::Bootstrap(const NActors::TActorContext& ctx)
{
    SelfVDiskId = GInfo->GetVDiskId(Config->BaseInfo.VDiskIdShort);

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "🚀 DDISK ACTOR BOOTSTRAP: Starting for VDiskId=" << SelfVDiskId.ToString()
        << " VDiskSlotId=" << Config->BaseInfo.VDiskSlotId
        << " UseInMemoryStorage=" << UseInMemoryStorage
        << " UseDirectDeviceIO=" << UseDirectDeviceIO
        << " actorId=" << ctx.SelfID.ToString());

    // Initialize PDisk connection
    InitializePDisk(ctx);

    Become(&TThis::StateWork);
}

void TDDiskActorImpl::HandleReadRequest(
    const TEvBlobStorage::TEvDDiskReadRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "🔥 DDISK HANDLE READ REQUEST ENTRY: sender=" << ev->Sender.ToString()
        << " cookie=" << ev->Cookie
        << " recipient=" << ev->Recipient.ToString());
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
        << " UseInMemoryStorage=" << UseInMemoryStorage
        << " UseDirectDeviceIO=" << UseDirectDeviceIO
        << " BlockDevice=" << (void*)BlockDevice);

    // IN-MEMORY STORAGE: Process read immediately without PDisk
    if (UseInMemoryStorage) {
        auto response = std::make_unique<TEvBlobStorage::TEvDDiskReadResponse>();
        response->Record.SetOffset(offset);
        response->Record.SetSize(size);
        response->Record.SetChunkId(chunkId);

        // Check if chunk exists in memory
        auto chunkIt = InMemoryChunks.find(chunkId);
        if (chunkIt != InMemoryChunks.end()) {
            // Read data from in-memory chunk
            TString data = chunkIt->second.ReadData(offset, size);
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
        } else {
            // Chunk not found, return zeros (uninitialized data)
            response->Record.SetData(TString(size, '\0'));
            response->Record.SetStatus(NKikimrProto::OK);

            LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
                "📥 IN-MEMORY READ RESULT: reqId=" << NextRequestCookie
                << " chunkId=" << chunkId << " offset=" << offset << " size=" << size
                << " status=OK_UNINITIALIZED_CHUNK (returning zeros)");
        }

        ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
        NextRequestCookie++;
        return;
    }

    // DIRECT DEVICE I/O PATH: Use async block device interface
    if (UseDirectDeviceIO) {
        // Check if we have block device for direct I/O
        if (!BlockDevice) {
            LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
                "DDiskActorImpl: Direct device I/O enabled but no block device available");
            auto response = std::make_unique<TEvBlobStorage::TEvDDiskReadResponse>();
            response->Record.SetStatus(NKikimrProto::ERROR);
            response->Record.SetErrorReason("No block device available for direct I/O");
            response->Record.SetOffset(offset);
            response->Record.SetSize(size);
            response->Record.SetChunkId(chunkId);
            response->Record.SetData(TString(size, 0));
            ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
            return;
        }

        // Check if chunk is known and has device offset info
        auto chunkIt = ChunkInfoMap.find(chunkId);
        if (chunkIt == ChunkInfoMap.end()) {
            LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
                "DDiskActorImpl: No device offset info for chunk " << chunkId);
            auto response = std::make_unique<TEvBlobStorage::TEvDDiskReadResponse>();
            response->Record.SetStatus(NKikimrProto::ERROR);
            response->Record.SetErrorReason("No device offset info for chunk");
            response->Record.SetOffset(offset);
            response->Record.SetSize(size);
            response->Record.SetChunkId(chunkId);
            response->Record.SetData(TString(size, 0));
            ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
            return;
        }

        // Extract original request ID for traceability (if passed via cookie)
        ui64 originalRequestId = ev->Cookie;  // Используем cookie как ID запроса
        TString traceIdStr = ev->TraceId.GetHexTraceId();  // Для логирования trace id

        // Calculate the actual device offset (chunk offset + offset within chunk)
        ui64 actualDeviceOffset = chunkIt->second.DeviceOffset + offset;

        // CRITICAL: Block devices require device offset to be 512-byte aligned
        ui64 alignedDeviceOffset = actualDeviceOffset & ~511ULL;  // Round down to 512-byte boundary
        ui32 offsetAdjustment = static_cast<ui32>(actualDeviceOffset - alignedDeviceOffset);

        // Adjust read size to include the offset adjustment
        ui32 adjustedSize = size + offsetAdjustment;
        ui32 alignedSize = (adjustedSize + 511) & ~511;  // Round up to nearest 512 bytes

        LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
            "🔍 DIRECT READ ALIGNMENT: chunkId=" << chunkId << " originalOffset=" << offset
            << " originalSize=" << size << " deviceOffset=" << chunkIt->second.DeviceOffset
            << " actualDeviceOffset=" << actualDeviceOffset
            << " alignedDeviceOffset=" << alignedDeviceOffset
            << " offsetAdjustment=" << offsetAdjustment
            << " adjustedSize=" << adjustedSize << " alignedSize=" << alignedSize);

        // Create aligned buffer for reading data with proper alignment
        char* alignedData = static_cast<char*>(aligned_alloc(512, alignedSize));
        if (!alignedData) {
            LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
                "DDiskActorImpl: Failed to allocate aligned buffer for direct read, size=" << alignedSize);
            auto response = std::make_unique<TEvBlobStorage::TEvDDiskReadResponse>();
            response->Record.SetStatus(NKikimrProto::ERROR);
            response->Record.SetErrorReason("Failed to allocate aligned buffer");
            response->Record.SetOffset(offset);
            response->Record.SetSize(size);
            response->Record.SetChunkId(chunkId);
            response->Record.SetData(TString(size, 0));
            ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
            return;
        }

        // Zero-initialize the buffer to avoid reading garbage data
        memset(alignedData, 0, alignedSize);

        // Create simple completion handler that properly manages the buffer
        // Pass the internal cookie, not the original one, so we can clean up the pending request
        // Create a copy of TraceId before the event is destroyed
        auto traceIdCopy = ev->TraceId.Clone();
        auto completion = new TDirectIOCompletion(ctx.SelfID, ev->Sender, ev->Cookie, originalRequestId, chunkId, offset, size, offsetAdjustment, true /*isRead*/, alignedData, alignedSize, traceIdCopy);
        LOG_DEBUG_S(ctx, NKikimrServices::BS_DEVICE,
            "🔧 DDIRECT COMPLETION CREATED: VDiskSlotId=" << Config->BaseInfo.VDiskSlotId
            << " action=" << (void*)completion << " isRead=true" << " TraceId=" << traceIdStr
            << " RequestId=" << originalRequestId);

        // Safety checks to prevent crashes
        if (alignedSize == 0 || alignedSize > (1024 * 1024 * 1024)) {  // Max 1GB
            LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
                "❌ INVALID ALIGNED SIZE: reqId=" << 0
                << " alignedSize=" << alignedSize << " - aborting operation");
            delete completion;
            auto response = std::make_unique<TEvBlobStorage::TEvDDiskReadResponse>();
            response->Record.SetStatus(NKikimrProto::ERROR);
            response->Record.SetErrorReason("Invalid aligned size");
            response->Record.SetOffset(offset);
            response->Record.SetSize(size);
            response->Record.SetChunkId(chunkId);
            response->Record.SetData(TString(size, 0));
            ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
            return;
        }

        LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
            "📨 STARTING DIRECT READ: reqId=" << originalRequestId
            << " internalCookie=" << ev->Cookie
            << " chunkId=" << chunkId << " offset=" << offset << " size=" << size
            << " alignedSize=" << alignedSize << " offsetAdjustment=" << offsetAdjustment
            << " vDiskId=" << SelfVDiskId.ToString()
            << " vSlotId=" << Config->BaseInfo.VDiskSlotId
            << " deviceOffset=" << chunkIt->second.DeviceOffset
            << " alignedDeviceOffset=" << alignedDeviceOffset
            << " bufferPtr=" << (void*)alignedData
            << " BlockDevice=" << (void*)BlockDevice);

        // Perform asynchronous direct read from block device with aligned offset and size
        try {
            BlockDevice->PreadAsync(alignedData, alignedSize, alignedDeviceOffset,
                                   completion, NPDisk::TReqId(), &traceIdCopy);
        } catch (const std::exception& e) {
            LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
                "❌ EXCEPTION in PreadAsync: " << e.what());
            delete completion;
            auto response = std::make_unique<TEvBlobStorage::TEvDDiskReadResponse>();
            response->Record.SetStatus(NKikimrProto::ERROR);
            response->Record.SetErrorReason("Exception in PreadAsync");
            response->Record.SetOffset(offset);
            response->Record.SetSize(size);
            response->Record.SetChunkId(chunkId);
            response->Record.SetData(TString(size, 0));
            ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
            return;
        } catch (...) {
            LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
                "❌ UNKNOWN EXCEPTION in PreadAsync");
            delete completion;
            auto response = std::make_unique<TEvBlobStorage::TEvDDiskReadResponse>();
            response->Record.SetStatus(NKikimrProto::ERROR);
            response->Record.SetErrorReason("Unknown exception in PreadAsync");
            response->Record.SetOffset(offset);
            response->Record.SetSize(size);
            response->Record.SetChunkId(chunkId);
            response->Record.SetData(TString(size, 0));
            ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
            return;
        }
        return;
    }

    // ORIGINAL PDisk PATH (disabled when UseInMemoryStorage=true)
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

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "🔥 DDISK HANDLE READ REQUEST EXIT: sender=" << ev->Sender.ToString()
        << " cookie=" << ev->Cookie);
}

void TDDiskActorImpl::HandleWriteRequest(
    const TEvBlobStorage::TEvDDiskWriteRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "🔥 DDISK HANDLE WRITE REQUEST ENTRY: sender=" << ev->Sender.ToString()
        << " cookie=" << ev->Cookie
        << " recipient=" << ev->Recipient.ToString());
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
        << " UseInMemoryStorage=" << UseInMemoryStorage
        << " UseDirectDeviceIO=" << UseDirectDeviceIO
        << " BlockDevice=" << (void*)BlockDevice
        << " dataSize=" << data.size());

    // Log data prefix/suffix for verification
    TString dataLogPrefix, dataLogSuffix;
    if (data.size() >= 32) {
        for (int i = 0; i < std::min<int>(16, data.size()); i++) {
            dataLogPrefix += TStringBuilder() << " " << (ui32)(ui8)data[i];
        }
        if (data.size() > 16) {
            for (int i = std::max<int>(16, static_cast<int>(data.size()-16)); i < static_cast<int>(data.size()); i++) {
                dataLogSuffix += TStringBuilder() << " " << (ui32)(ui8)data[i];
            }
        }
    }

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

    // IN-MEMORY STORAGE: Process write immediately without PDisk
    if (UseInMemoryStorage) {
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
        return;
    }

    // DIRECT DEVICE I/O PATH: Use async block device interface
    if (UseDirectDeviceIO) {
        // Check if we have block device for direct I/O
        if (!BlockDevice) {
            LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
                "DDiskActorImpl: Direct device I/O enabled but no block device available");
            auto response = std::make_unique<TEvBlobStorage::TEvDDiskWriteResponse>();
            response->Record.SetStatus(NKikimrProto::ERROR);
            response->Record.SetErrorReason("No block device available for direct I/O");
            response->Record.SetOffset(offset);
            response->Record.SetSize(size);
            response->Record.SetChunkId(chunkId);
            ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
            return;
        }

        // Check if chunk is known and has device offset info
        auto chunkIt = ChunkInfoMap.find(chunkId);
        if (chunkIt == ChunkInfoMap.end()) {
            LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
                "DDiskActorImpl: No device offset info for chunk " << chunkId);
            auto response = std::make_unique<TEvBlobStorage::TEvDDiskWriteResponse>();
            response->Record.SetStatus(NKikimrProto::ERROR);
            response->Record.SetErrorReason("No device offset info for chunk");
            response->Record.SetOffset(offset);
            response->Record.SetSize(size);
            response->Record.SetChunkId(chunkId);
            ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
            return;
        }

        // Extract original request ID for traceability (if passed via cookie)
        ui64 originalRequestId = ev->Cookie;  // Используем cookie как ID запроса
        TString traceIdStr = ev->TraceId.GetHexTraceId();  // Для логирования trace id

        // Calculate the actual device offset (chunk offset + offset within chunk)
        ui64 actualDeviceOffset = chunkIt->second.DeviceOffset + offset;

        // CRITICAL: Block devices require device offset to be 512-byte aligned
        ui64 alignedDeviceOffset = actualDeviceOffset & ~511ULL;  // Round down to 512-byte boundary
        ui32 offsetAdjustment = static_cast<ui32>(actualDeviceOffset - alignedDeviceOffset);

        // For writes, we need to read-modify-write if not aligned
        bool needsReadModifyWrite = (offsetAdjustment != 0 || (data.size() & 511) != 0);
        ui32 alignedSize = ((data.size() + offsetAdjustment + 511) & ~511);  // Round up to nearest 512 bytes

        LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
            "🔍 DIRECT WRITE ALIGNMENT: reqId=" << 0
            << " chunkId=" << chunkId << " originalOffset=" << offset << " originalSize=" << size
            << " deviceOffset=" << chunkIt->second.DeviceOffset
            << " actualDeviceOffset=" << actualDeviceOffset
            << " alignedDeviceOffset=" << alignedDeviceOffset
            << " offsetAdjustment=" << offsetAdjustment
            << " dataSize=" << data.size() << " alignedSize=" << alignedSize
            << " needsReadModifyWrite=" << needsReadModifyWrite);

        // Create aligned buffer for writing data with proper alignment
        char* alignedData = static_cast<char*>(aligned_alloc(512, alignedSize));
        if (!alignedData) {
            LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
                "DDiskActorImpl: Failed to allocate aligned buffer for direct write, size=" << alignedSize);
            auto response = std::make_unique<TEvBlobStorage::TEvDDiskWriteResponse>();
            response->Record.SetStatus(NKikimrProto::ERROR);
            response->Record.SetErrorReason("Failed to allocate aligned buffer");
            response->Record.SetOffset(offset);
            response->Record.SetSize(size);
            response->Record.SetChunkId(chunkId);
            ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
            return;
        }

        // Initialize the aligned buffer
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

        // Create simple completion handler that properly manages the buffer
        // Create a copy of TraceId before the event is destroyed
        auto traceIdCopy = ev->TraceId.Clone();
        auto completion = new TDirectIOCompletion(ctx.SelfID, ev->Sender, ev->Cookie, originalRequestId, chunkId, offset, size, offsetAdjustment, false /*isRead*/, alignedData, alignedSize, traceIdCopy);
        LOG_DEBUG_S(ctx, NKikimrServices::BS_DEVICE,
            "🔧 DDIRECT COMPLETION CREATED: VDiskSlotId=" << Config->BaseInfo.VDiskSlotId
            << " action=" << (void*)completion << " isRead=false" << " TraceId=" << traceIdStr
            << " RequestId=" << originalRequestId);

        // Safety checks to prevent crashes
        if (alignedSize == 0 || alignedSize > (1024 * 1024 * 1024)) {  // Max 1GB
            LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
                "❌ INVALID ALIGNED SIZE: reqId=" << 0
                << " alignedSize=" << alignedSize << " - aborting operation");
            delete completion;
            auto response = std::make_unique<TEvBlobStorage::TEvDDiskWriteResponse>();
            response->Record.SetStatus(NKikimrProto::ERROR);
            response->Record.SetErrorReason("Invalid aligned size");
            response->Record.SetOffset(offset);
            response->Record.SetSize(size);
            response->Record.SetChunkId(chunkId);
            ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
            return;
        }

        LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
            "📨 STARTING DIRECT WRITE: reqId=" << originalRequestId
            << " Cookie=" << ev->Cookie
            << " chunkId=" << chunkId << " offset=" << offset << " size=" << size
            << " alignedSize=" << alignedSize << " offsetAdjustment=" << offsetAdjustment
            << " vDiskId=" << SelfVDiskId.ToString()
            << " vSlotId=" << Config->BaseInfo.VDiskSlotId
            << " deviceOffset=" << chunkIt->second.DeviceOffset
            << " alignedDeviceOffset=" << alignedDeviceOffset
            << " dataSize=" << data.size()
            << " bufferPtr=" << (void*)alignedData
            << " BlockDevice=" << (void*)BlockDevice);

        // Perform asynchronous direct write to block device with aligned offset and size
        try {
            BlockDevice->PwriteAsync(alignedData, alignedSize, alignedDeviceOffset,
                                    completion, NPDisk::TReqId(), &traceIdCopy);
        } catch (const std::exception& e) {
            LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
                "❌ EXCEPTION in PwriteAsync: " << e.what());
            delete completion;
            auto response = std::make_unique<TEvBlobStorage::TEvDDiskWriteResponse>();
            response->Record.SetStatus(NKikimrProto::ERROR);
            response->Record.SetErrorReason("Exception in PwriteAsync");
            response->Record.SetOffset(offset);
            response->Record.SetSize(size);
            response->Record.SetChunkId(chunkId);
            ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
            return;
        } catch (...) {
            LOG_ERROR_S(ctx, NKikimrServices::BS_DDISK,
                "❌ UNKNOWN EXCEPTION in PwriteAsync");
            delete completion;
            auto response = std::make_unique<TEvBlobStorage::TEvDDiskWriteResponse>();
            response->Record.SetStatus(NKikimrProto::ERROR);
            response->Record.SetErrorReason("Unknown exception in PwriteAsync");
            response->Record.SetOffset(offset);
            response->Record.SetSize(size);
            response->Record.SetChunkId(chunkId);
            ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
            return;
        }
        return;
    }

    // ORIGINAL PDisk PATH (disabled when UseInMemoryStorage=true)
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

    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "🔥 DDISK HANDLE WRITE REQUEST EXIT (PDisk path): sender=" << ev->Sender.ToString()
        << " cookie=" << ev->Cookie);
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
        reservationCookie,  // Pass cookie to PDisk
        true  // UseRawChunk for block device storage
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
            << " cookie=" << 0
            << " pendingCount=" << PendingRequests.size());
        return;
    }

    const TPendingRequest& request = it->second;

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
    LOG_DEBUG_S(ctx, NKikimrServices::BS_DDISK,
        "🚀 SENDING DDISK READ RESPONSE: reqId=" << 0
        << " to=" << request.Sender.ToString() << " cookie=" << reinterpret_cast<uintptr_t>(request.Cookie)
        << " status=" << NKikimrProto::EReplyStatus_Name(response->Record.GetStatus())
        << " dataSize=" << response->Record.GetData().size()
        << " chunkId=" << response->Record.GetChunkId()
        << " vDiskId=" << SelfVDiskId.ToString());

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
            << " cookie=" << 0
            << " pendingCount=" << PendingRequests.size());
        return;
    }

    const TPendingRequest& request = it->second;

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

void TDDiskActorImpl::TDirectIOCompletion::Exec(TActorSystem *actorSystem) {
    // Prevent double execution using atomic flag
    bool expected = false;
    if (!ExecutedOrReleased.compare_exchange_strong(expected, true)) {
        LOG_ERROR_S(*actorSystem, NKikimrServices::BS_DDISK,
            "⚠️ TDirectIOCompletion::Exec DOUBLE EXECUTION PREVENTED: this=" << (void*)this
            << " already executed/released"
            << " Result=" << (ui32)Result << " ChunkIdx=" << ChunkIdx << " IsRead=" << IsRead
            << " RequestId=" << RequestId << " OriginalCookie=" << OriginalCookie
            << " AlignedBuffer=" << (void*)AlignedBuffer
            << " OriginalSender=" << OriginalSender.ToString() << " traceId=" << TraceId.GetHexTraceId());
        return;
    }

    LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_DDISK,
        "🔧 TDirectIOCompletion::Exec ENTER: this=" << (void*)this
        << " Result=" << (ui32)Result << " ChunkIdx=" << ChunkIdx << " IsRead=" << IsRead
        << " RequestId=" << RequestId << " OriginalCookie=" << OriginalCookie
        << " AlignedBuffer=" << (void*)AlignedBuffer
        << " OriginalSender=" << OriginalSender.ToString() << " traceId=" << TraceId.GetHexTraceId()
        << " ExecutedOrReleased=" << ExecutedOrReleased.load());

    NKikimrProto::EReplyStatus status = (Result == NPDisk::EIoResult::Ok) ? NKikimrProto::OK : NKikimrProto::ERROR;

    if (IsRead) {
        // Handle read completion - send TEvDDiskReadResponse directly to original sender
        auto response = std::make_unique<TEvBlobStorage::TEvDDiskReadResponse>();
        response->Record.SetStatus(status);
        response->Record.SetOffset(OriginalOffset);
        response->Record.SetSize(OriginalSize);
        response->Record.SetChunkId(ChunkIdx);

        if (Result != NPDisk::EIoResult::Ok) {
            response->Record.SetErrorReason(ErrorReason);
        }

        if (Result == NPDisk::EIoResult::Ok && AlignedBuffer) {
            LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_DDISK,
                "📖 TDirectIOCompletion::Exec setting read data: this=" << (void*)this
                << " AlignedBuffer=" << (void*)AlignedBuffer << " OriginalSize=" << OriginalSize
                << " OffsetAdjustment=" << OffsetAdjustment << " AlignedSize=" << AlignedSize);

            // Extract data from aligned buffer at the correct position
            char* actualDataStart = AlignedBuffer + OffsetAdjustment;

            // Verify we don't read beyond the buffer
            if (OffsetAdjustment + OriginalSize > AlignedSize) {
                LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_DDISK,
                    "⚠️ TDirectIOCompletion::Exec buffer overflow protection: OffsetAdjustment="
                    << OffsetAdjustment << " OriginalSize=" << OriginalSize << " AlignedSize=" << AlignedSize);

                ui32 safeSize = Min(OriginalSize, AlignedSize - OffsetAdjustment);
                response->Record.SetData(TString(actualDataStart, safeSize));
            } else {
                response->Record.SetData(TString(actualDataStart, OriginalSize));
            }
        }

        LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_DDISK,
            "🚀 SENDING DDISK READ RESPONSE: this=" << (void*)this
            << " to=" << OriginalSender.ToString()
            << " cookie=" << OriginalCookie
            << " status=" << (ui32)status
            << " dataSize=" << (response->Record.HasData() ? response->Record.GetData().size() : 0)
            << " traceId=" << TraceId.GetHexTraceId());

        actorSystem->Send(new IEventHandle(OriginalSender, TActorId(), response.release(), 0, OriginalCookie, nullptr, std::move(TraceId)));
    } else {
        // Handle write completion - send TEvDDiskWriteResponse directly to original sender
        auto response = std::make_unique<TEvBlobStorage::TEvDDiskWriteResponse>();
        response->Record.SetStatus(status);
        response->Record.SetOffset(OriginalOffset);
        response->Record.SetSize(OriginalSize);
        response->Record.SetChunkId(ChunkIdx);

        if (Result != NPDisk::EIoResult::Ok) {
            response->Record.SetErrorReason(ErrorReason);
        }

        LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_DDISK,
            "🚀 SENDING DDISK WRITE RESPONSE: this=" << (void*)this
            << " to=" << OriginalSender.ToString()
            << " cookie=" << OriginalCookie
            << " status=" << (ui32)status
            << " traceId=" << TraceId.GetHexTraceId());

        actorSystem->Send(new IEventHandle(OriginalSender, TActorId(), response.release(), 0, OriginalCookie, nullptr, std::move(TraceId)));
    }

    LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_DDISK,
        "📤 TDirectIOCompletion::Exec completed: this=" << (void*)this
        << " OriginalSender=" << OriginalSender.ToString() << " status=" << (ui32)status
        << " traceId=" << TraceId.GetHexTraceId());

    // Сохраняем значения для логирования перед удалением объекта
    const void* thisPtr = this;
    ui64 requestId = RequestId;
    ui32 chunkIdx = ChunkIdx;
    bool isRead = IsRead;
    TString traceIdStr = TraceId.GetHexTraceId();

    // Логирование перед удалением объекта
    LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_DDISK,
        "💀 TDirectIOCompletion::Exec OBJECT DELETING: this=" << (void*)this
        << " RequestId=" << RequestId << " ChunkIdx=" << ChunkIdx << " IsRead=" << IsRead
        << " traceId=" << TraceId.GetHexTraceId()
        << " ExecutedOrReleased=" << ExecutedOrReleased.load());

    // Удаляем объект здесь - PDisk вызывает либо Exec, либо Release, но не оба
    // Буфер будет освобожден в деструкторе
    delete this;

    // Логирование после удаления объекта с использованием сохраненных значений
    LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_DDISK,
        "💀 TDirectIOCompletion::Exec OBJECT DELETED: this=" << thisPtr
        << " RequestId=" << requestId << " ChunkIdx=" << chunkIdx << " IsRead=" << isRead
        << " traceId=" << traceIdStr);
}

void TDDiskActorImpl::TDirectIOCompletion::Release(TActorSystem *actorSystem) {
    // Prevent double release using atomic flag
    bool expected = false;
    if (!ExecutedOrReleased.compare_exchange_strong(expected, true)) {
        LOG_ERROR_S(*actorSystem, NKikimrServices::BS_DDISK,
            "⚠️ TDirectIOCompletion::Release DOUBLE RELEASE PREVENTED: this=" << (void*)this
            << " already executed/released"
            << " Result=" << (ui32)Result << " ChunkIdx=" << ChunkIdx << " IsRead=" << IsRead
            << " RequestId=" << RequestId << " OriginalCookie=" << OriginalCookie
            << " AlignedBuffer=" << (void*)AlignedBuffer
            << " OriginalSender=" << OriginalSender.ToString() << " traceId=" << TraceId.GetHexTraceId());
        return;
    }

    LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_DDISK,
        "🔧 TDirectIOCompletion::Release ENTER: this=" << (void*)this
        << " Result=" << (ui32)Result << " ChunkIdx=" << ChunkIdx << " IsRead=" << IsRead
        << " RequestId=" << RequestId << " OriginalCookie=" << OriginalCookie
        << " AlignedBuffer=" << (void*)AlignedBuffer
        << " OriginalSender=" << OriginalSender.ToString() << " traceId=" << TraceId.GetHexTraceId()
        << " ExecutedOrReleased=" << ExecutedOrReleased.load());

    // Send error response directly to original sender
    if (IsRead) {
        auto response = std::make_unique<TEvBlobStorage::TEvDDiskReadResponse>();
        response->Record.SetStatus(NKikimrProto::ERROR);
        response->Record.SetOffset(OriginalOffset);
        response->Record.SetSize(OriginalSize);
        response->Record.SetChunkId(ChunkIdx);
        response->Record.SetErrorReason(ErrorReason);
        actorSystem->Send(new IEventHandle(OriginalSender, TActorId(), response.release(), 0, OriginalCookie, nullptr, std::move(TraceId)));
    } else {
        auto response = std::make_unique<TEvBlobStorage::TEvDDiskWriteResponse>();
        response->Record.SetStatus(NKikimrProto::ERROR);
        response->Record.SetOffset(OriginalOffset);
        response->Record.SetSize(OriginalSize);
        response->Record.SetChunkId(ChunkIdx);
        response->Record.SetErrorReason(ErrorReason);
        actorSystem->Send(new IEventHandle(OriginalSender, TActorId(), response.release(), 0, OriginalCookie, nullptr, std::move(TraceId)));
    }

    LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_DDISK,
        "📤 TDirectIOCompletion::Release sent error response: this=" << (void*)this
        << " OriginalSender=" << OriginalSender.ToString() << " ErrorReason=" << ErrorReason
        << " traceId=" << TraceId.GetHexTraceId());

    // Сохраняем значения для логирования перед удалением объекта
    const void* thisPtr = this;
    ui64 requestId = RequestId;
    ui32 chunkIdx = ChunkIdx;
    bool isRead = IsRead;
    TString traceIdStr = TraceId.GetHexTraceId();

    // Логирование перед удалением объекта
    LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_DDISK,
        "💀 TDirectIOCompletion::Release OBJECT DELETING: this=" << (void*)this
        << " RequestId=" << RequestId << " ChunkIdx=" << ChunkIdx << " IsRead=" << IsRead
        << " traceId=" << TraceId.GetHexTraceId()
        << " ExecutedOrReleased=" << ExecutedOrReleased.load());

    // Теперь безопасно удаляем объект - PDisk больше не будет его использовать
    delete this;

    // Логирование после удаления объекта с использованием сохраненных значений
    LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_DDISK,
        "💀 TDirectIOCompletion::Release OBJECT DELETED: this=" << thisPtr
        << " RequestId=" << requestId << " ChunkIdx=" << chunkIdx << " IsRead=" << isRead
        << " traceId=" << traceIdStr);
}


}   // namespace NKikimr
