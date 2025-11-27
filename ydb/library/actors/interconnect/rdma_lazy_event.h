#pragma once

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/util/rc_buf.h>
#include <ydb/library/actors/wilson/wilson_trace.h>

namespace NActors {

////////////////////////////////////////////////////////////////////////////////
// Lazy RDMA Event for Deferred Deserialization
// 
// This event carries raw RDMA payload + serialized metadata without
// deserializing in the CQ thread, deferring expensive operations (~100us)
// to the target actor's thread.
////////////////////////////////////////////////////////////////////////////////

struct TEvRdmaLazyWrite : TEventLocal<TEvRdmaLazyWrite, 10001> {
    // RDMA payload buffer (directly from RDMA transfer, no copy)
    TRcBuf Payload;

    // Serialized protobuf metadata for the original event
    TString SerializedMetadata;

    // Event type embedded in RDMA command (for validation/routing)
    ui32 EmbeddedEventType;

    // Actor IDs for routing
    TActorId ResponseTarget;
    TActorId FinalTarget;

    // Cookie for request tracking
    ui64 Cookie;

    // Wilson tracing ID
    NWilson::TTraceId TraceId;

    TEvRdmaLazyWrite() = default;

    TEvRdmaLazyWrite(
        TRcBuf&& payload,
        TString&& serializedMetadata,
        ui32 embeddedEventType,
        TActorId responseTarget,
        TActorId finalTarget,
        ui64 cookie,
        NWilson::TTraceId traceId)
        : Payload(std::move(payload))
        , SerializedMetadata(std::move(serializedMetadata))
        , EmbeddedEventType(embeddedEventType)
        , ResponseTarget(responseTarget)
        , FinalTarget(finalTarget)
        , Cookie(cookie)
        , TraceId(std::move(traceId))
    {}

    TString ToString() const override {
        TStringStream str;
        str << "TEvRdmaLazyWrite {Cookie# " << Cookie
            << " PayloadSize# " << Payload.size()
            << " MetadataSize# " << SerializedMetadata.size()
            << " EventType# " << EmbeddedEventType
            << " FinalTarget# " << FinalTarget.ToString()
            << " ResponseTarget# " << ResponseTarget.ToString() << "}";
        return str.Str();
    }
};

} // namespace NActors

