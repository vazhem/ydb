#include "ddisk_actor_mode_direct_completion.h"

#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>

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

    // Create child span for Exec() execution
    NWilson::TSpan childSpan = Span.CreateChild(TWilson::BlobStorage, 
        IsRead ? "DDisk.Read.TDirectIOCompletion.Exec" : "DDisk.Write.TDirectIOCompletion.Exec");

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

        // Save child trace ID before sending (avoid accessing after move)
        NWilson::TTraceId childTraceId = childSpan.GetTraceId();
        TString childTraceIdHex = childTraceId.GetHexTraceId();

        LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_DDISK,
            "🚀 SENDING DDISK READ RESPONSE: this=" << (void*)this
            << " to=" << OriginalSender.ToString()
            << " cookie=" << OriginalCookie
            << " status=" << (ui32)status
            << " dataSize=" << (response->Record.HasData() ? response->Record.GetData().size() : 0)
            << " traceId=" << childTraceIdHex);

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

        actorSystem->Send(new IEventHandle(OriginalSender, TActorId(), response.release(), 0, OriginalCookie, nullptr, std::move(childTraceId)));
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

        // Save child trace ID before sending (avoid accessing after move)
        NWilson::TTraceId childTraceId = childSpan.GetTraceId();
        TString childTraceIdHex = childTraceId.GetHexTraceId();

        LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_DDISK,
            "🚀 SENDING DDISK WRITE RESPONSE: this=" << (void*)this
            << " to=" << OriginalSender.ToString()
            << " cookie=" << OriginalCookie
            << " status=" << (ui32)status
            << " traceId=" << childTraceIdHex);

        // End Wilson span before sending response
        if (Span) {
            if (status == NKikimrProto::OK) {
                Span.EndOk();
            } else {
                Span.EndError(ErrorReason);
            }
        }

        actorSystem->Send(new IEventHandle(OriginalSender, TActorId(), response.release(), 0, OriginalCookie, nullptr, std::move(childTraceId)));
    }

    LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_DDISK,
        "📤 TDirectIOCompletion::Exec completed: this=" << (void*)this
        << " OriginalSender=" << OriginalSender.ToString() << " status=" << (ui32)status
        << " traceId=" << TraceId.GetHexTraceId());

    // End child span before deleting object
    if (status == NKikimrProto::OK) {
        childSpan.EndOk();
    } else {
        childSpan.EndError(ErrorReason);
    }

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

    // Send error response directly to original sender
    if (IsRead) {
        auto response = std::make_unique<TEvBlobStorage::TEvDDiskReadResponse>();
        response->Record.SetStatus(NKikimrProto::ERROR);
        response->Record.SetOffset(OriginalOffset);
        response->Record.SetSize(OriginalSize);
        response->Record.SetChunkId(ChunkIdx);
        response->Record.SetErrorReason(ErrorReason);
        actorSystem->Send(new IEventHandle(OriginalSender, TActorId(), response.release(), 0, OriginalCookie, nullptr, std::move(TraceId)));

        LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_DDISK,
            "📤 TDirectIOCompletion::Release sent error response: this=" << (void*)this
            << " OriginalSender=" << OriginalSender.ToString() << " ErrorReason=" << ErrorReason
            << " traceId=" << releaseTraceIdHex);
    } else {
        auto response = std::make_unique<TEvBlobStorage::TEvDDiskWriteResponse>();
        response->Record.SetStatus(NKikimrProto::ERROR);
        response->Record.SetOffset(OriginalOffset);
        response->Record.SetSize(OriginalSize);
        response->Record.SetChunkId(ChunkIdx);
        response->Record.SetErrorReason(ErrorReason);
        actorSystem->Send(new IEventHandle(OriginalSender, TActorId(), response.release(), 0, OriginalCookie, nullptr, std::move(TraceId)));
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
