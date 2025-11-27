#pragma once

#include <ydb/library/actors/core/events.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <functional>
#include <memory>

namespace NActors {

////////////////////////////////////////////////////////////////////////////////
// RDMA Event Serialization
//
// Helper functions to serialize/deserialize events for RDMA transfer.
// Events are serialized as: [EventType:4 bytes][Cookie:8 bytes][PayloadSize:4 bytes][Payload]
////////////////////////////////////////////////////////////////////////////////

struct TRdmaEventMetadata {
    ui32 EventType;
    ui64 Cookie;
    ui32 PayloadSize;
    TActorId Source;
    TActorId Target;

    TString Serialize() const;
    static bool Deserialize(const TString& data, TRdmaEventMetadata& meta);
};

// Serialize event for RDMA transfer (without payload data)
TString SerializeEventMetadata(
    IEventBase* event,
    ui32 eventType,
    ui64 cookie,
    TActorId source,
    TActorId target);

// Register event factory for RDMA event reconstruction
// This allows other modules to register their event types without circular dependencies
void RegisterEventFactory(
    ui32 eventType,
    std::function<std::unique_ptr<IEventBase>()> factory);

// Reconstruct event from metadata and payload
std::unique_ptr<IEventBase> ReconstructEvent(
    const TRdmaEventMetadata& meta,
    const TString& payload);

// Create event instance by type (using registered factory)
std::unique_ptr<IEventBase> CreateEventByType(ui32 eventType);

} // namespace NActors
