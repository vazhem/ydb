#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/wilson/wilson_trace.h>

namespace NActors {

////////////////////////////////////////////////////////////////////////////////
// RDMA-Aware Transport Functions
//
// These functions transparently use RDMA when available, otherwise fall back to TCP.
// The key difference from regular Send is that they understand the semantics
// of read/write operations and can optimize the data path accordingly.
////////////////////////////////////////////////////////////////////////////////

// Send write request via RDMA if available
// For write operations:
// 1. Sends TEvRdmaWriteCommand with metadata to target
// 2. Target auto-executes RDMA_READ to fetch data
// 3. Target reconstructs TEvDDiskWriteRequest locally
// 4. Target processes write and sends ack back
bool RdmaAwareWrite(
    const TActorContext& ctx,
    TActorId target,
    IEventBase* event,
    ui32 flags = 0,
    ui64 cookie = 0,
    NWilson::TTraceId traceId = {});

// Send write request via RDMA if available (explicit actor system version)
bool RdmaAwareWrite(
    TActorSystem* actorSystem,
    TActorId source,
    TActorId target,
    IEventBase* event,
    ui32 flags = 0,
    ui64 cookie = 0,
    NWilson::TTraceId traceId = {});

// Send read request via RDMA if available
// For read operations:
// 1. Sends TEvRdmaReadCommand with metadata to target
// 2. Target reads data from device and auto-executes RDMA_WRITE to source
// 3. Source receives data and ack
bool RdmaAwareRead(
    const TActorContext& ctx,
    TActorId target,
    IEventBase* event,
    ui32 flags = 0,
    ui64 cookie = 0,
    NWilson::TTraceId traceId = {});

// Send read request via RDMA if available (explicit actor system version)
bool RdmaAwareRead(
    TActorSystem* actorSystem,
    TActorId source,
    TActorId target,
    IEventBase* event,
    ui32 flags = 0,
    ui64 cookie = 0,
    NWilson::TTraceId traceId = {});

// Generic send function (for backward compatibility)
// Tries to detect operation type from event
bool RdmaAwareSend(
    const TActorContext& ctx,
    TActorId target,
    IEventBase* event,
    ui32 flags = 0,
    ui64 cookie = 0,
    NWilson::TTraceId traceId = {});

// Generic send function (explicit actor system version)
bool RdmaAwareSend(
    TActorSystem* actorSystem,
    TActorId source,
    TActorId target,
    IEventBase* event,
    ui32 flags = 0,
    ui64 cookie = 0,
    NWilson::TTraceId traceId = {});

} // namespace NActors
