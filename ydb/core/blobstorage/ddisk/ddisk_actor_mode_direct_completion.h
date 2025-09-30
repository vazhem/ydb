#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_completion.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/ddisk/ddisk_events.h>
#include <ydb/library/actors/wilson/wilson_trace.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <util/generic/hash.h>
#include <memory>

namespace NKikimr {

//
// Simple direct I/O completion handler that doesn't interfere with PDisk buffer management
//
class TDirectIOCompletion : public NPDisk::TCompletionAction {
private:
    TActorId DDiskActorId;  // DDisk actor ID for logging
    TActorId OriginalSender;  // The original request sender
    ui64 OriginalCookie;     // The original request cookie
    ui64 RequestId;          // The request ID for traceability
    TChunkIdx ChunkIdx;
    ui32 OriginalOffset;
    ui32 OriginalSize;
    ui32 OffsetAdjustment;
    bool IsRead;
    char* AlignedBuffer;  // Simple aligned buffer using aligned_alloc
    ui32 AlignedSize;
    mutable std::atomic<bool> ExecutedOrReleased{false};
    NWilson::TSpan Span;  // Wilson tracing span for this operation

public:
    TDirectIOCompletion(const TActorId& ddiskActorId, const TActorId& originalSender,
                       ui64 originalCookie, ui64 requestId, TChunkIdx chunkIdx, ui32 originalOffset,
                       ui32 originalSize, ui32 offsetAdjustment, bool isRead,
                       char* alignedBuffer, ui32 alignedSize, NWilson::TTraceId traceId)
        : DDiskActorId(ddiskActorId)
        , OriginalSender(originalSender)
        , OriginalCookie(originalCookie)
        , RequestId(requestId)
        , ChunkIdx(chunkIdx)
        , OriginalOffset(originalOffset)
        , OriginalSize(originalSize)
        , OffsetAdjustment(offsetAdjustment)
        , IsRead(isRead)
        , AlignedBuffer(alignedBuffer)
        , AlignedSize(alignedSize)
        , Span(TWilson::BlobStorage, std::move(traceId), isRead ? "DDisk.DirectReadCompletion" : "DDisk.DirectWriteCompletion")
    {
        // Initialize base class TCompletionAction fields
        OperationIdx = 0;  // Will be set by PDisk when operation is scheduled
        SubmitTime = 0;
        FlushAction = nullptr;
        CostNs = 0;
        Result = NPDisk::EIoResult::Unknown;
        ErrorReason = "";
        TraceId = NWilson::TTraceId(Span.GetTraceId());
    }

    virtual ~TDirectIOCompletion() {
        if (AlignedBuffer) {
            free(AlignedBuffer);
            AlignedBuffer = nullptr;
        }
    }

    char* Data() const { return AlignedBuffer; }
    ui32 Size() const { return AlignedSize; }

    bool CanHandleResult() const override {
        // Always handle result, even for errors - we want to send responses
        return true;
    }

    void Exec(TActorSystem* actorSystem) override;
    void Release(TActorSystem* actorSystem) override;
};

}   // namespace NKikimr
