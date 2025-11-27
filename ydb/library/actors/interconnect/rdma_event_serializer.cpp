#include "rdma_event_serializer.h"
#include "rdma_event_base.h"
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/protos/services_common.pb.h>

namespace NActors {

// Forward declarations for event types
// This avoids circular dependencies between interconnect and core
// These are only needed in the factory registry, not in the main namespace

////////////////////////////////////////////////////////////////////////////////
// TRdmaEventMetadata Implementation
////////////////////////////////////////////////////////////////////////////////

TString TRdmaEventMetadata::Serialize() const {
    TString result;
    result.reserve(sizeof(EventType) + sizeof(Cookie) + sizeof(PayloadSize) +
                   sizeof(TActorId) * 2);

    // Write event type
    result.append(reinterpret_cast<const char*>(&EventType), sizeof(EventType));

    // Write cookie
    result.append(reinterpret_cast<const char*>(&Cookie), sizeof(Cookie));

    // Write payload size
    result.append(reinterpret_cast<const char*>(&PayloadSize), sizeof(PayloadSize));

    // Write source actor ID
    result.append(reinterpret_cast<const char*>(&Source), sizeof(Source));

    // Write target actor ID
    result.append(reinterpret_cast<const char*>(&Target), sizeof(Target));

    return result;
}

bool TRdmaEventMetadata::Deserialize(const TString& data, TRdmaEventMetadata& meta) {
    size_t expectedSize = sizeof(meta.EventType) + sizeof(meta.Cookie) +
                          sizeof(meta.PayloadSize) + sizeof(TActorId) * 2;

    if (data.size() < expectedSize) {
        return false;
    }

    const char* ptr = data.data();

    // Read event type
    memcpy(&meta.EventType, ptr, sizeof(meta.EventType));
    ptr += sizeof(meta.EventType);

    // Read cookie
    memcpy(&meta.Cookie, ptr, sizeof(meta.Cookie));
    ptr += sizeof(meta.Cookie);

    // Read payload size
    memcpy(&meta.PayloadSize, ptr, sizeof(meta.PayloadSize));
    ptr += sizeof(meta.PayloadSize);

    // Read source actor ID
    memcpy(&meta.Source, ptr, sizeof(meta.Source));
    ptr += sizeof(meta.Source);

    // Read target actor ID
    memcpy(&meta.Target, ptr, sizeof(meta.Target));

    return true;
}

////////////////////////////////////////////////////////////////////////////////
// Event Serialization Functions
////////////////////////////////////////////////////////////////////////////////

TString SerializeEventMetadata(
    IEventBase* event,
    ui32 eventType,
    ui64 cookie,
    TActorId source,
    TActorId target)
{
    // First, serialize the event-specific data using IRdmaPassable interface
    TString eventData;

    auto* rdmaPassable = AsRdmaPassable(event);
    if (rdmaPassable) {
        eventData = rdmaPassable->SerializeMetadata();
        Cerr << "SerializeEventMetadata: Event type " << eventType
             << " serialized to " << eventData.size() << " bytes" << Endl;
    } else {
        Cerr << "SerializeEventMetadata: Event type " << eventType
             << " doesn't implement IRdmaPassable" << Endl;
    }

    // Create metadata header
    TRdmaEventMetadata meta;
    meta.EventType = eventType;
    meta.Cookie = cookie;
    meta.PayloadSize = eventData.size();
    meta.Source = source;
    meta.Target = target;

    // Combine header + event data
    TString result = meta.Serialize();
    result.append(eventData);

    Cerr << "SerializeEventMetadata: Total size = " << result.size()
         << " (header=" << meta.Serialize().size() << " + eventData=" << eventData.size() << ")" << Endl;

    return result;
}

////////////////////////////////////////////////////////////////////////////////
// Event factory registry
// This allows other modules to register their event types without creating
// circular dependencies
////////////////////////////////////////////////////////////////////////////////

namespace {
    using TEventFactory = std::function<std::unique_ptr<IEventBase>()>;
    using TEventFactoryMap = std::unordered_map<ui32, TEventFactory>;

    TEventFactoryMap& GetEventFactoryMap() {
        static TEventFactoryMap factoryMap;
        return factoryMap;
    }
}

void RegisterEventFactory(ui32 eventType, std::function<std::unique_ptr<IEventBase>()> factory) {
    GetEventFactoryMap()[eventType] = std::move(factory);
}

std::unique_ptr<IEventBase> CreateEventByType(ui32 eventType) {
    auto& factoryMap = GetEventFactoryMap();

    // Log available factories for debugging
    char hexBuf[16];
    snprintf(hexBuf, sizeof(hexBuf), "0x%08X", eventType);
    Cerr << "CreateEventByType: Looking for factory for event type " << eventType
         << " (" << hexBuf << "), available factories: " << factoryMap.size() << Endl;

    auto it = factoryMap.find(eventType);
    if (it != factoryMap.end()) {
        Cerr << "CreateEventByType: Found factory for event type " << eventType << Endl;
        return it->second();
    }

    Cerr << "CreateEventByType: No factory found for event type " << eventType << Endl;
    return nullptr;
}

std::unique_ptr<IEventBase> ReconstructEvent(
    const TRdmaEventMetadata& meta,
    const TString& payload)
{
    // Create event by type
    auto event = CreateEventByType(meta.EventType);
    if (!event) {
        Cerr << "ReconstructEvent: Failed to create event of type " << meta.EventType << Endl;
        return nullptr;
    }

    // Check if event supports RDMA (implements IRdmaPassable)
    auto* rdmaPassable = AsRdmaPassable(event.get());
    if (!rdmaPassable) {
        Cerr << "ReconstructEvent: Event type " << meta.EventType << " doesn't implement IRdmaPassable" << Endl;
        return nullptr;
    }

    // Deserialize metadata using IRdmaPassable interface
    Cerr << "ReconstructEvent: Deserializing metadata for event type " << meta.EventType
         << ", payload size: " << payload.size() << Endl;
    if (!rdmaPassable->DeserializeMetadata(payload)) {
        Cerr << "ReconstructEvent: Failed to deserialize metadata for event type " << meta.EventType << Endl;
        return nullptr;
    }

    Cerr << "ReconstructEvent: Successfully reconstructed event type " << meta.EventType << Endl;

    // Note: The actual data payload will be populated separately
    // after RDMA transfer completes using SetRdmaPayload()

    return event;
}

} // namespace NActors
