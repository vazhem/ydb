#include "ddisk_actor_mode_direct_completion.h"

#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <ydb/library/actors/interconnect/interconnect_rdma_api.h>

namespace NKikimr {

void TDirectIOCompletion::Exec(TActorSystem *actorSystem) {
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
        // Handle read completion - check if RDMA metadata is present
        if (HasRdmaMetadata && Result == NPDisk::EIoResult::Ok) {
            LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_DDISK,
                "🔧 TDirectIOCompletion::Exec RDMA-aware read completion: this=" << (void*)this
                << " RdmaResponseAddr=" << (void*)RdmaResponseAddr
                << " RdmaResponseRkey=" << RdmaResponseRkey
                << " OriginalSize=" << OriginalSize);

            // Create response with data for RDMA transfer
            auto response = std::make_unique<TEvBlobStorage::TEvDDiskReadResponse>();
            response->Record.SetStatus(status);
            response->Record.SetOffset(OriginalOffset);
            response->Record.SetSize(OriginalSize);
            response->Record.SetChunkId(ChunkIdx);

            // Extract data from aligned buffer and set as payload
            char* actualDataStart = AlignedBuffer + OffsetAdjustment;
            ui32 dataSize = OriginalSize;
            if (OffsetAdjustment + OriginalSize > AlignedSize) {
                dataSize = Min(OriginalSize, AlignedSize - OffsetAdjustment);
            }
            response->StorePayload(TRope(TString(actualDataStart, dataSize)));

            // End Wilson span before RDMA operation
            if (Span) {
                Span.EndOk();
            }

            // Use RdmaAwareReadCompletion to transfer data via RDMA WRITE
            bool rdmaSuccess = NActors::RdmaAwareReadCompletion(
                actorSystem,
                DDiskActorId,      // Target actor (this DDisk)
                OriginalSender,    // Source actor (original requester)
                response.release(), // Response event with payload
                OriginalCookie,    // Original request cookie
                RdmaResponseAddr,  // Destination address on source node
                RdmaResponseRkey,  // Destination rkey
                OriginalSize,      // Destination size
                TraceId.GetTraceId()            // Trace ID for distributed tracing
            );

            if (!rdmaSuccess) {
                LOG_ERROR_S(*actorSystem, NKikimrServices::BS_DDISK,
                    "❌ TDirectIOCompletion::Exec RdmaAwareReadCompletion failed: this=" << (void*)this);
                // Fall back to regular response
                auto errorResponse = std::make_unique<TEvBlobStorage::TEvDDiskReadResponse>();
                errorResponse->Record.SetStatus(NKikimrProto::ERROR);
                errorResponse->Record.SetErrorReason("RDMA transfer failed");
                errorResponse->Record.SetOffset(OriginalOffset);
                errorResponse->Record.SetSize(OriginalSize);
                errorResponse->Record.SetChunkId(ChunkIdx);
                actorSystem->Send(new IEventHandle(OriginalSender, TActorId(), errorResponse.release(), 0, OriginalCookie, nullptr, std::move(TraceId)));
            }
        } else {
            // Regular (non-RDMA) read completion
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

            // End Wilson span before sending response
            if (Span) {
                if (status == NKikimrProto::OK) {
                    LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_DDISK, "DDISK completiton span end ok");
                    Span.EndOk();
                } else {
                    Span.EndError(ErrorReason);
                    LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_DDISK, "DDISK completiton span end error");
                }
            } else {
                LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_DDISK, "DDISK completiton no span");
            }

            actorSystem->Send(new IEventHandle(OriginalSender, TActorId(), response.release(), 0, OriginalCookie, nullptr, std::move(TraceId)));
        }
    } else {
        // Handle write completion - send TEvDDiskWriteResponse via RDMA
        auto response = std::make_unique<TEvBlobStorage::TEvDDiskWriteResponse>();
        response->Record.SetStatus(status);
        response->Record.SetOffset(OriginalOffset);
        response->Record.SetSize(OriginalSize);
        response->Record.SetChunkId(ChunkIdx);

        if (Result != NPDisk::EIoResult::Ok) {
            response->Record.SetErrorReason(ErrorReason);
        }

        LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_DDISK,
            "🚀 SENDING DDISK WRITE RESPONSE via RDMA: this=" << (void*)this
            << " to=" << OriginalSender.ToString()
            << " cookie=" << OriginalCookie
            << " status=" << (ui32)status
            << " traceId=" << TraceId.GetHexTraceId());

        // End Wilson span before sending response
        auto childTraceId = Span.GetTraceId();
        if (Span) {
            if (status == NKikimrProto::OK) {
                Span.EndOk();
            } else {
                Span.EndError(ErrorReason);
            }
        }

        // Use RdmaWriteCompletion to send response via RDMA SEND
        // Now works for all TEventPB events after removing artificial restriction
        bool rdmaSuccess = NActors::RdmaWriteCompletion(
            actorSystem,
            DDiskActorId,      // Source actor (this DDisk)
            OriginalSender,    // Target actor (original requester)
            response.release(), // Response event (TEventPB-based)
            OriginalCookie,    // Original request cookie
            std::move(childTraceId)            // Trace ID for distributed tracing
        );

        if (!rdmaSuccess) {
            LOG_ERROR_S(*actorSystem, NKikimrServices::BS_DDISK,
                "❌ TDirectIOCompletion::Exec RdmaWriteCompletion failed: this=" << (void*)this
                << " falling back to TCP");
            // RdmaWriteCompletion already falls back to TCP on failure, so no need to resend
        }
    }

    LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_DDISK,
        "📤 TDirectIOCompletion::Exec completed: this=" << (void*)this
        << " OriginalSender=" << OriginalSender.ToString() << " status=" << (ui32)status
        << " traceId=" << TraceId.GetHexTraceId());


    // Логирование перед удалением объекта
    LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_DDISK,
        "💀 TDirectIOCompletion::Exec OBJECT DELETING: this=" << (void*)this
        << " RequestId=" << RequestId << " ChunkIdx=" << ChunkIdx << " IsRead=" << IsRead
        << " traceId=" << TraceId.GetHexTraceId()
        << " ExecutedOrReleased=" << ExecutedOrReleased.load());

    // Удаляем объект здесь - PDisk вызывает либо Exec, либо Release, но не оба
    // Буфер будет освобожден в деструкторе
    delete this;
}

void TDirectIOCompletion::Release(TActorSystem *actorSystem) {
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

    // Save trace ID string before any moves in Release method
    TString releaseTraceIdHex = TraceId.GetHexTraceId();

    // End Wilson span before sending error responses
    if (Span) {
        Span.EndError(ErrorReason);
    }

    // Send error response via RDMA to original sender
    if (IsRead) {
        auto response = std::make_unique<TEvBlobStorage::TEvDDiskReadResponse>();
        response->Record.SetStatus(NKikimrProto::ERROR);
        response->Record.SetOffset(OriginalOffset);
        response->Record.SetSize(OriginalSize);
        response->Record.SetChunkId(ChunkIdx);
        response->Record.SetErrorReason(ErrorReason);

        // For read errors, just send directly (no RDMA metadata in error case)
        actorSystem->Send(new IEventHandle(OriginalSender, TActorId(), response.release(), 0, OriginalCookie, nullptr, std::move(TraceId)));

        LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_DDISK,
            "📤 TDirectIOCompletion::Release sent read error response: this=" << (void*)this
            << " OriginalSender=" << OriginalSender.ToString() << " ErrorReason=" << ErrorReason
            << " traceId=" << releaseTraceIdHex);
    } else {
        auto response = std::make_unique<TEvBlobStorage::TEvDDiskWriteResponse>();
        response->Record.SetStatus(NKikimrProto::ERROR);
        response->Record.SetOffset(OriginalOffset);
        response->Record.SetSize(OriginalSize);
        response->Record.SetChunkId(ChunkIdx);
        response->Record.SetErrorReason(ErrorReason);

        LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_DDISK,
            "📤 TDirectIOCompletion::Release sending write error response via RDMA: this=" << (void*)this
            << " OriginalSender=" << OriginalSender.ToString() << " ErrorReason=" << ErrorReason
            << " traceId=" << releaseTraceIdHex);

        // Use RdmaWriteCompletion for write error responses too
        bool rdmaSuccess = NActors::RdmaWriteCompletion(
            actorSystem,
            DDiskActorId,      // Source actor (this DDisk)
            OriginalSender,    // Target actor (original requester)
            response.release(), // Error response event
            OriginalCookie,    // Original request cookie
            TraceId.GetTraceId()            // Trace ID for distributed tracing
        );

        if (!rdmaSuccess) {
            LOG_ERROR_S(*actorSystem, NKikimrServices::BS_DDISK,
                "❌ TDirectIOCompletion::Release RdmaWriteCompletion failed: this=" << (void*)this);
            // RdmaWriteCompletion already falls back to TCP on failure
        }
    }

    // Логирование перед удалением объекта
    LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_DDISK,
        "💀 TDirectIOCompletion::Release OBJECT DELETING: this=" << (void*)this
        << " RequestId=" << RequestId << " ChunkIdx=" << ChunkIdx << " IsRead=" << IsRead
        << " traceId=" << TraceId.GetHexTraceId()
        << " ExecutedOrReleased=" << ExecutedOrReleased.load());

    // Теперь безопасно удаляем объект - PDisk больше не будет его использовать
    delete this;
}

}   // namespace NKikimr
