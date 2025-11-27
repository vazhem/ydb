#include "rdma_recv_handler.h"
#include "rdma_cq_command_handler.h"
#include "rdma_event_serializer.h"
#include "rdma_data_transfer_events.h"
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <ydb/library/wilson_ids/wilson.h>

namespace NActors {

void TRdmaRecvHandler::HandleRecvCompletion(
    TActorSystem* actorSystem,
    const ibv_wc* wc,
    void* recvBuffer,
    size_t recvSize,
    std::shared_ptr<NInterconnect::NRdma::TQueuePair> qp,
    NInterconnect::NRdma::ICq::TPtr cq)
{
    Y_UNUSED(wc);

    // Validate input parameters
    if (!recvBuffer) {
        LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaRecvHandler: Null recv buffer");
        return;
    }

    if (recvSize == 0) {
        LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaRecvHandler: Zero recv size");
        return;
    }

    // Validate received size is reasonable
    // NOTE: For inline sends (IBV_SEND_INLINE), the data is copied by the sender's NIC
    // and written into our RECV buffer. From our perspective, inline vs. non-inline
    // is completely transparent - we just see data in our buffer with wc->byte_len size.
    if (recvSize > 4096) {
        LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaRecvHandler: Received size too large: %zu bytes (max 4096)",
            recvSize);
        return;
    }

    LOG_DEBUG(*actorSystem, NActorsServices::INTERCONNECT,
        "RdmaRecvHandler: Processing %zu bytes from RECV buffer (supports both inline and registered sends)",
        recvSize);

    DispatchReceivedData(
        actorSystem,
        static_cast<const char*>(recvBuffer),
        recvSize,
        std::move(qp),
        std::move(cq));
}

void TRdmaRecvHandler::DispatchReceivedData(
    TActorSystem* actorSystem,
    const char* data,
    size_t size,
    std::shared_ptr<NInterconnect::NRdma::TQueuePair> qp,
    NInterconnect::NRdma::ICq::TPtr cq)
{
    // RDMA messages use TRdmaMessageHeader (lightweight version of TEventDescr2) followed by serialized event data
    // This matches the actor system's approach: event descriptor + event data

    if (size < sizeof(TRdmaMessageHeader)) {
        LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaRecvHandler: Received data too small for header: size=%zu (need at least %zu)",
            size, sizeof(TRdmaMessageHeader));
        return;
    }

    // Parse TRdmaMessageHeader to get event metadata
    const TRdmaMessageHeader* header = reinterpret_cast<const TRdmaMessageHeader*>(data);
    ui32 eventType = header->Type;
    ui32 payloadSize = size - sizeof(TRdmaMessageHeader);  // Calculate payload size from total size
    TActorId recipient = header->Recipient;
    TActorId sender = header->Sender;
    ui64 cookie = header->Cookie;
    ui32 flags = header->Flags;
    NWilson::TTraceId traceId(header->TraceId);  // Deserialize TraceId from header

    NWilson::TSpan span(
        NKikimr::TWilson::BlobStorage,
        std::move(traceId.Clone()),
        "RDMA.HandleEvent",
        NWilson::EFlags::NONE,
        actorSystem
    );

    LOG_DEBUG(*actorSystem, NActorsServices::INTERCONNECT,
        "RdmaRecvHandler: Received message type=%u, payloadSize=%u, totalSize=%zu, recipient=%s, sender=%s, cookie=%lu",
        eventType, payloadSize, size, recipient.ToString().c_str(), sender.ToString().c_str(), cookie);

    // Validate total size
    if (sizeof(TRdmaMessageHeader) + payloadSize != size) {
        LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaRecvHandler: Size mismatch: header=%zu + payload=%u = %zu, but received %zu",
            sizeof(TRdmaMessageHeader), payloadSize, sizeof(TRdmaMessageHeader) + payloadSize, size);
        return;
    }

    // Create TEventSerializedData from payload (after header)
    const char* payload = data + sizeof(TRdmaMessageHeader);
    TRope rope;
    rope.Insert(rope.End(), TRcBuf::Copy(payload, payloadSize));

    auto serializedData = MakeIntrusive<TEventSerializedData>(
        std::move(rope), TEventSerializationInfo{});

    // Deserialize based on event type
    switch (eventType) {
        case EvRdmaWriteCommand: {
            auto* writeCmd = TEvRdmaWriteCommand::Load(serializedData.Get());
            if (!writeCmd) {
                LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
                    "RdmaRecvHandler: Failed to deserialize TEvRdmaWriteCommand");
                return;
            }

            LOG_DEBUG(*actorSystem, NActorsServices::INTERCONNECT,
                "RdmaRecvHandler: Received TEvRdmaWriteCommand cookie=%lu", writeCmd->GetCookie());

            // Create span for RDMA write command processing
            NWilson::TSpan childSpan = span.CreateChild(NKikimr::TWilson::BlobStorage, "RDMA.HandleEvent.Write");

            TRdmaCqCommandHandler::HandleWriteCommand(
                actorSystem, writeCmd, std::move(qp), std::move(cq), std::move(childSpan));
            break;
        }

        case EvRdmaReadCommand: {
            auto* readCmd = TEvRdmaReadCommand::Load(serializedData.Get());
            if (!readCmd) {
                LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
                    "RdmaRecvHandler: Failed to deserialize TEvRdmaReadCommand");
                return;
            }

            LOG_DEBUG(*actorSystem, NActorsServices::INTERCONNECT,
                "RdmaRecvHandler: Received TEvRdmaReadCommand cookie=%lu", readCmd->GetCookie());

            // Create span for RDMA read command processing
            NWilson::TSpan childSpan = span.CreateChild(NKikimr::TWilson::BlobStorage, "RDMA.HandleEvent.Read");

            TRdmaCqCommandHandler::HandleReadCommand(
                actorSystem, readCmd, std::move(qp), std::move(cq), std::move(childSpan));
            break;
        }

        default: {
            // General event - deserialize using TEventSerializedData and forward to recipient
            LOG_INFO(*actorSystem, NActorsServices::INTERCONNECT,
                "RdmaRecvHandler: Received general event type=%u, forwarding from %s to %s, cookie=%lu",
                eventType, sender.ToString().c_str(), recipient.ToString().c_str(), cookie);

            // Create an IEventHandle with serialized data and metadata from TRdmaMessageHeader
            // The actor system will deserialize it when the recipient processes it
            auto handle = new IEventHandle(
                eventType,
                flags,          // Use flags from header
                recipient,      // Recipient from header
                sender,         // Sender from header
                serializedData, // Serialized payload
                cookie,         // Cookie from header
                nullptr,        // Forward recipient
                std::move(traceId)  // TraceId from header
            );

            if (!handle) {
                LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
                    "RdmaRecvHandler: Failed to create event handle for type=%u", eventType);
                return;
            }

            LOG_DEBUG(*actorSystem, NActorsServices::INTERCONNECT,
                "RdmaRecvHandler: Forwarding event type=%u from %s to %s with cookie=%lu",
                eventType, sender.ToString().c_str(), recipient.ToString().c_str(), cookie);

            // Send the event to the recipient actor
            actorSystem->Send(handle);
            break;
        }
    }

    span.EndOk();
}

} // namespace NActors
