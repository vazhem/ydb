#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/interconnect/rdma/rdma.h>
#include <ydb/library/actors/interconnect/rdma/mem_pool.h>
#include <ydb/library/actors/wilson/wilson_trace.h>
#include <optional>
#include <functional>
#include <memory>

namespace NActors {

////////////////////////////////////////////////////////////////////////////////
// RDMA Connection Handle
////////////////////////////////////////////////////////////////////////////////

struct TRdmaConnectionHandle {
    std::shared_ptr<NInterconnect::NRdma::TQueuePair> Qp;
    std::shared_ptr<NInterconnect::NRdma::ICq> Cq;
    ui32 PeerQpNum;
    ui64 PeerGid[2];
    ssize_t RdmaDeviceIndex;

    TRdmaConnectionHandle() = default;

    TRdmaConnectionHandle(
        std::shared_ptr<NInterconnect::NRdma::TQueuePair> qp,
        std::shared_ptr<NInterconnect::NRdma::ICq> cq,
        ui32 peerQpNum,
        const ui64 peerGid[2],
        ssize_t rdmaDeviceIndex)
        : Qp(std::move(qp))
        , Cq(std::move(cq))
        , PeerQpNum(peerQpNum)
        , RdmaDeviceIndex(rdmaDeviceIndex)
    {
        PeerGid[0] = peerGid[0];
        PeerGid[1] = peerGid[1];
    }
};

////////////////////////////////////////////////////////////////////////////////
// Public API Functions
////////////////////////////////////////////////////////////////////////////////

// Function to get RDMA connection details for a given peer actor
std::optional<TRdmaConnectionHandle> GetRdmaConnection(
    TActorSystem* actorSystem,
    TActorId peerActorId);

// Function to send data via RDMA (similar to actor system Send)
bool RdmaSend(
    TActorSystem* actorSystem,
    TActorId sourceActorId,
    TActorId targetActorId,
    IEventBase* event,
    ui64 cookie = 0,
    NWilson::TTraceId traceId = {});

// Function to post an RDMA Read operation
bool PostRdmaRead(
    TActorSystem* actorSystem,
    TActorId sourceActorId, // The actor on the remote node that owns the source memory
    void* sourceAddr,
    ui32 sourceRkey,
    void* destAddr,
    ui32 destLkey,
    ui32 destSize,
    std::function<void(TActorSystem* as, NInterconnect::NRdma::TEvRdmaIoDone*)> callback,
    NWilson::TTraceId traceId = {});

// Function to post an RDMA Write operation
bool PostRdmaWrite(
    TActorSystem* actorSystem,
    TActorId targetActorId, // The actor on the remote node that owns the target memory
    void* sourceAddr,
    ui32 sourceLkey,
    ui32 sourceSize,
    void* destAddr,
    ui32 destRkey,
    std::function<void(TActorSystem* as, NInterconnect::NRdma::TEvRdmaIoDone*)> callback,
    NWilson::TTraceId traceId = {});

////////////////////////////////////////////////////////////////////////////////
// RDMA-Aware Read API for DDisk Operations
////////////////////////////////////////////////////////////////////////////////

// RdmaAwareRead: Send a read request to target DDisk actor
// This function:
// 1. Allocates RDMA buffer for response data on source side
// 2. Sends read request metadata via RDMA to target
// 3. Target reconstructs request and sends to local DDisk actor
// Returns the cookie for tracking this request
bool RdmaAwareRead(
    TActorSystem* actorSystem,
    TActorId sourceActor,      // Source actor ID (for response delivery)
    TActorId targetActor,      // Target DDisk actor ID
    IEventBase* requestEvent,  // Request event (must implement IRdmaPassable)
    ui64 cookie,               // Cookie for matching request/response
    ui32 expectedResponseSize, // Expected response data size
    NWilson::TTraceId traceId = {}); // Trace ID for distributed tracing

// RdmaAwareReadCompletion: Send read response with data via RDMA WRITE
// This function is called by the target DDisk actor after reading data
// It performs RDMA WRITE to transfer data directly to source memory
// The destination region info comes from the original request (RdmaResponseAddr/Rkey in TEvDDiskReadRequest)
bool RdmaAwareReadCompletion(
    TActorSystem* actorSystem,
    TActorId targetActor,      // Target actor ID (this DDisk actor)
    TActorId sourceActor,      // Source actor ID (original requester)
    IEventBase* responseEvent, // Response event with data (must implement IRdmaPassable)
    ui64 cookie,               // Original request cookie
    ui64 destAddress,          // Destination address on source node
    ui32 destRkey,             // Destination rkey for RDMA write
    ui32 destSize,             // Destination buffer size
    NWilson::TTraceId traceId = {}); // Trace ID for distributed tracing

// RdmaWriteCompletion: Send write completion notification via RDMA SEND
// This is a wrapper over RdmaSend for API consistency with read flow
// Use this after DDisk completes a write operation to notify the source
bool RdmaWriteCompletion(
    TActorSystem* actorSystem,
    TActorId srcActor,         // Source actor ID (this DDisk actor)
    TActorId tgtActor,         // Target actor ID (original requester)
    IEventBase* event,         // Completion event (typically TEvDDiskWriteResponse)
    ui64 cookie,               // Original request cookie
    NWilson::TTraceId traceId = {}); // Trace ID for distributed tracing

// Helper: Check if a request came via RDMA and get context for completion
// Returns true if this is an RDMA request and fills the context parameters
// DDisk actors should call this to determine if they need to use RdmaAwareReadCompletion
bool GetRdmaReadContext(
    ui64 cookie,               // Request cookie
    ui64& destAddress,         // OUT: Destination address on source node
    ui32& destRkey,            // OUT: Destination rkey
    ui32& destSize,            // OUT: Destination size
    TActorId& sourceActor,     // OUT: Source actor ID
    TActorId& targetActor);    // OUT: Target actor ID

} // namespace NActors
