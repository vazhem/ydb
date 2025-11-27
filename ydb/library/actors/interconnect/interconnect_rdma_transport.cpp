#include "interconnect_rdma_transport.h"
#include "interconnect_rdma_api.h"
#include "rdma_data_transfer_events.h"
#include "rdma_event_serializer.h"
#include "rdma_event_base.h"
#include "rdma_memory_region.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/protos/services_common.pb.h>

namespace NActors {

////////////////////////////////////////////////////////////////////////////////
// Helper: Extract payload from event
////////////////////////////////////////////////////////////////////////////////


////////////////////////////////////////////////////////////////////////////////
// RdmaAwareWrite Implementation
////////////////////////////////////////////////////////////////////////////////

bool RdmaAwareWrite(
    const TActorContext& ctx,
    TActorId target,
    IEventBase* event,
    ui32 flags,
    ui64 cookie,
    NWilson::TTraceId traceId)
{
    return RdmaAwareWrite(ctx.ActorSystem(), ctx.SelfID, target, event, flags, cookie, std::move(traceId));
}

bool RdmaAwareWrite(
    TActorSystem* actorSystem,
    TActorId source,
    TActorId target,
    IEventBase* event,
    ui32 flags,
    ui64 cookie,
    NWilson::TTraceId traceId)
{
    // Check if target is on a different node
    const ui32 targetNodeId = target.NodeId();
    const ui32 localNodeId = actorSystem->NodeId;

    if (targetNodeId == localNodeId || targetNodeId == 0) {
        // Local send, use standard actor system
        actorSystem->Send(new IEventHandle(target, source, event, flags, cookie, nullptr, std::move(traceId)));
        return false;
    }

    // Check if RDMA connection is available
    auto rdmaConn = GetRdmaConnection(actorSystem, target);
    if (!rdmaConn) {
        // No RDMA available, use standard IC
        actorSystem->Send(new IEventHandle(target, source, event, flags, cookie, nullptr, std::move(traceId)));
        return false;
    }

    // Check if event implements IRdmaPassable
    auto* rdmaPassable = AsRdmaPassable(event);
    if (!rdmaPassable) {
        // Event doesn't support RDMA, use regular send
        LOG_DEBUG(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaAwareWrite: Event doesn't implement IRdmaPassable, using TCP for %s", target.ToString().c_str());
        actorSystem->Send(new IEventHandle(target, source, event, flags, cookie, nullptr, std::move(traceId)));
        return false;
    }

    // Extract payload using IRdmaPassable interface
    TRope payload = rdmaPassable->GetRdmaPayload();
    if (payload.GetSize() == 0) {
        // No payload, use regular send
        LOG_DEBUG(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaAwareWrite: Event has no payload, using TCP for %s", target.ToString().c_str());
        actorSystem->Send(new IEventHandle(target, source, event, flags, cookie, nullptr, std::move(traceId)));
        return false;
    }

    // Check if payload is RDMA-allocated
    auto rdmaRegion = ExtractRdmaRegion(payload, rdmaConn->RdmaDeviceIndex);
    if (!rdmaRegion) {
        // Not RDMA memory, use regular send
        LOG_DEBUG(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaAwareWrite: Payload not RDMA-allocated (device=%zd), using TCP for %s",
            rdmaConn->RdmaDeviceIndex, target.ToString().c_str());
        actorSystem->Send(new IEventHandle(target, source, event, flags, cookie, nullptr, std::move(traceId)));
        return false;
    }

    LOG_INFO(*actorSystem, NActorsServices::INTERCONNECT,
        "RdmaAwareWrite: Payload IS RDMA-allocated! Sending RDMA write command to %s, size=%u, addr=0x%lx, rkey=%u (device=%zd)",
        target.ToString().c_str(), rdmaRegion->Size, rdmaRegion->Address, rdmaRegion->Rkey, rdmaConn->RdmaDeviceIndex);

    // Serialize event metadata (without payload) using protobuf
    TString metadata = rdmaPassable->SerializeMetadata();
    ui32 eventType = event->Type();

    // Create RDMA write command with embedded event serialized in ApplicationPayload
    auto rdmaCmd = std::make_unique<TEvRdmaWriteCommand>(
        cookie,              // cookie
        source,              // responseTarget (where to send ack)
        *rdmaRegion,         // sourceRegion (where target will read from)
        std::move(metadata), // applicationPayload (serialized embedded event protobuf)
        eventType,           // embeddedEventType (for reconstruction on receiver)
        target,              // finalTarget (where to deliver reconstructed event)
        true                 // autoExecuteRdmaRead (CQ will auto-fetch data)
    );

    // Send via RDMA
    bool sent = RdmaSend(
        actorSystem,
        source,
        target,
        rdmaCmd.release(),
        cookie,
        std::move(traceId)   // Pass TraceId for distributed tracing
    );

    if (!sent) {
        // RDMA send failed, fallback to regular send
        LOG_WARN(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaAwareWrite: RDMA send failed for %s, falling back to TCP", target.ToString().c_str());
        actorSystem->Send(new IEventHandle(target, source, event, flags, cookie));
        return false;
    }

    // Successfully sent via RDMA
    // Note: Original event is now owned by RDMA layer and will be freed when done
    delete event;
    return true;
}

////////////////////////////////////////////////////////////////////////////////
// RdmaAwareRead Implementation
// Note: This is not implemented here because it requires knowledge of specific
// event types (NKikimr::TEvBlobStorage::TEvDDiskReadRequest) which creates
// circular dependencies between interconnect and core.
// It should be implemented in the ddisk module or a higher layer instead.
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
// RdmaAwareSend Implementation (Generic, for backward compatibility)
////////////////////////////////////////////////////////////////////////////////

bool RdmaAwareSend(
    const TActorContext& ctx,
    TActorId target,
    IEventBase* event,
    ui32 flags,
    ui64 cookie,
    NWilson::TTraceId traceId)
{
    return RdmaAwareSend(ctx.ActorSystem(), ctx.SelfID, target, event, flags, cookie, std::move(traceId));
}

bool RdmaAwareSend(
    TActorSystem* actorSystem,
    TActorId source,
    TActorId target,
    IEventBase* event,
    ui32 flags,
    ui64 cookie,
    NWilson::TTraceId traceId)
{
    Y_UNUSED(flags);

    // Check if target is on a different node
    const ui32 targetNodeId = target.NodeId();
    const ui32 localNodeId = actorSystem->NodeId;

    if (targetNodeId == localNodeId || targetNodeId == 0) {
        // Local send, use standard actor system
        actorSystem->Send(new IEventHandle(target, source, event, flags, cookie, nullptr, std::move(traceId)));
        return false;
    }

    // Check if RDMA connection is available
    auto rdmaConn = GetRdmaConnection(actorSystem, target);
    if (!rdmaConn) {
        // No RDMA available, use standard IC
        actorSystem->Send(new IEventHandle(target, source, event, flags, cookie, nullptr, std::move(traceId)));
        return false;
    }

    // Send the event via RDMA without auto-execution
    // The event is sent as-is to the remote side
    bool sent = RdmaSend(
        actorSystem,
        source,
        target,
        event,
        cookie,
        std::move(traceId)   // Pass TraceId for distributed tracing
    );

    if (!sent) {
        // RDMA send failed, fallback to regular send
        LOG_WARN(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaAwareSend: RDMA send failed for %s, falling back to TCP", target.ToString().c_str());
        actorSystem->Send(new IEventHandle(target, source, event, flags, cookie));
        return false;
    }

    // Successfully sent via RDMA
    return true;
}

} // namespace NActors
