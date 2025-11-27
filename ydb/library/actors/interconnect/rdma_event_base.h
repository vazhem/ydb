#pragma once

#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/util/rope.h>

namespace NActors {

////////////////////////////////////////////////////////////////////////////////
// Base class for events that can be passed through RDMA
// Similar to TEventPB, but provides additional interface for RDMA serialization
////////////////////////////////////////////////////////////////////////////////

class IRdmaPassable {
public:
    virtual ~IRdmaPassable() = default;

    // Get the payload data (for events with large data payloads)
    // Returns empty rope if event has no payload
    virtual TRope GetRdmaPayload() const = 0;

    // Set the payload data (used during reconstruction after RDMA transfer)
    // Should attach the payload to the event
    virtual void SetRdmaPayload(TRope&& payload) = 0;

    // Serialize the event metadata (without payload) for RDMA transfer
    // Uses the actor system's serialization approach via IEventBase::SerializeToArcadiaStream
    // This includes all fields except the large data payload
    // Note: For TEventPB-based events, this should strip the payload before serialization
    virtual TString SerializeMetadata() const = 0;

    // Deserialize the event metadata (without payload) from RDMA transfer
    // This should populate all fields except the large data payload
    // Note: The payload will be set separately via SetRdmaPayload after RDMA transfer
    virtual bool DeserializeMetadata(const TString& metadata) = 0;

    // Check if this event has payload that should be transferred via RDMA
    virtual bool HasRdmaPayload() const {
        return GetRdmaPayload().GetSize() > 0;
    }
};

////////////////////////////////////////////////////////////////////////////////
// Helper template for TEventPB-based events to implement IRdmaPassable
// Most DDisk events already inherit from TEventPB, so we need a way to
// add RDMA functionality to them
////////////////////////////////////////////////////////////////////////////////

// This is used as a marker interface that events should implement
// to be RDMA-passable. The actual implementation can use multiple inheritance
// or composition depending on the event structure.
//
// Usage example:
//
// For events already derived from TEventPB:
//
// struct TEvDDiskWriteRequest
//     : TEventPB<...>, IRdmaPassable {
//
//     TRope GetRdmaPayload() const override {
//         return GetItemBuffer();
//     }
//
//     void SetRdmaPayload(TRope&& payload) override {
//         StorePayload(std::move(payload));
//     }
//
//     TString SerializeMetadata() const override {
//         // Use actor system's serialization approach (without large payload)
//         // Strip payload before serialization to avoid sending it twice
//         TAllocChunkSerializer serializer;
//         if (!this->SerializeToArcadiaStream(&serializer)) {
//             return {};
//         }
//         auto data = serializer.Release(this->CreateSerializationInfo());
//         return data->GetString();
//     }
//
//     bool DeserializeMetadata(const TString& metadata) override {
//         // Use actor system's deserialization approach
//         TRope rope;
//         rope.Insert(rope.End(), TRcBuf::Copy(metadata.data(), metadata.size()));
//         auto serializedData = MakeIntrusive<TEventSerializedData>(
//             std::move(rope), TEventSerializationInfo{});
//
//         auto* loaded = TEvDDiskWriteRequest::Load(serializedData.Get());
//         if (!loaded) {
//             return false;
//         }
//
//         // Copy the loaded event's record to this instance
//         this->Record.CopyFrom(loaded->Record);
//         delete loaded;
//         return true;
//     }
// };

////////////////////////////////////////////////////////////////////////////////
// Helper functions to check if an event is RDMA-passable
////////////////////////////////////////////////////////////////////////////////

inline bool IsRdmaPassable(const IEventBase* event) {
    return dynamic_cast<const IRdmaPassable*>(event) != nullptr;
}

inline IRdmaPassable* AsRdmaPassable(IEventBase* event) {
    return dynamic_cast<IRdmaPassable*>(event);
}

inline const IRdmaPassable* AsRdmaPassable(const IEventBase* event) {
    return dynamic_cast<const IRdmaPassable*>(event);
}

} // namespace NActors
