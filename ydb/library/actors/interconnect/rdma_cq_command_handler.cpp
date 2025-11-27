#include "rdma_cq_command_handler.h"
#include "interconnect_rdma_api.h"
#include "rdma_event_serializer.h"
#include "rdma_event_base.h"
#include "rdma_memory_region.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <ydb/library/actors/interconnect/rdma/events.h>
#include <ydb/library/actors/util/rc_buf.h>

namespace NActors {

////////////////////////////////////////////////////////////////////////////////
// TRdmaCqCommandHandler Implementation
////////////////////////////////////////////////////////////////////////////////

void TRdmaCqCommandHandler::HandleWriteCommand(
    TActorSystem* actorSystem,
    TEvRdmaWriteCommand* cmd,
    std::shared_ptr<NInterconnect::NRdma::TQueuePair> qp,
    NInterconnect::NRdma::ICq::TPtr cq,
    NWilson::TSpan span)
{
    auto cmdCookie = cmd->GetCookie();
    auto cmdSourceRegion = cmd->GetSourceRegion();
    auto cmdResponseTarget = cmd->GetResponseTarget();
    auto cmdFinalTarget = cmd->GetFinalTarget();
    auto cmdAutoExecuteRdmaRead = cmd->GetAutoExecuteRdmaRead();

    LOG_INFO_S(*actorSystem, NActorsServices::INTERCONNECT,
        "RdmaCqCommandHandler::HandleWriteCommand cookie=" << cmdCookie
        << " sourceRegion={addr=" << cmdSourceRegion.Address
        << " rkey=" << cmdSourceRegion.Rkey
        << " size=" << cmdSourceRegion.Size << "}"
        << " autoExecute=" << cmdAutoExecuteRdmaRead
        << " finalTarget=" << cmdFinalTarget.ToString());

    if (!cmdAutoExecuteRdmaRead) {
        // Manual mode: just send the command to the final target
        // The target actor will handle the RDMA_READ itself
        span.EndOk();
        actorSystem->Send(new IEventHandle(
            cmdFinalTarget,
            cmdResponseTarget,
            cmd,  // Transfer ownership
            0, // flags
            cmdCookie,
            nullptr,
            span.GetTraceId()
        ));
        return;
    }

    // Auto-execute mode: perform RDMA_READ automatically
    if (!cmdSourceRegion.IsValid()) {
        LOG_ERROR_S(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaCqCommandHandler: Invalid source region for cookie=" << cmd->GetCookie());

        auto ack = std::make_unique<TEvRdmaWriteAck>(
            cmd->GetCookie(),
            false,
            "Invalid source memory region");
        actorSystem->Send(new IEventHandle(
            cmd->GetResponseTarget(),
            cmd->GetFinalTarget(),
            ack.release(),
            0, // flags
            cmd->GetCookie()
        ));

        delete cmd;
        return;
    }

    // Allocate local buffer for RDMA_READ using RcBufAllocator
    auto* allocator = actorSystem->GetRcBufAllocator();
    if (!allocator) {
        LOG_ERROR_S(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaCqCommandHandler: No RDMA allocator available");

        auto ack = std::make_unique<TEvRdmaWriteAck>(
            cmd->GetCookie(),
            false,
            "No RDMA allocator available");
        actorSystem->Send(new IEventHandle(
            cmd->GetResponseTarget(),
            cmd->GetFinalTarget(),
            ack.release(),
            0, // flags
            cmd->GetCookie()
        ));

        delete cmd;
        return;
    }

    std::optional<TRcBuf> localBufOpt = allocator->AllocRcBuf(cmd->GetSourceRegion().Size, 0, 0);
    if (!localBufOpt.has_value()) {
        LOG_ERROR_S(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaCqCommandHandler: Failed to allocate local buffer of size " << cmd->GetSourceRegion().Size);

        auto ack = std::make_unique<TEvRdmaWriteAck>(
            cmd->GetCookie(),
            false,
            "Failed to allocate local buffer");
        actorSystem->Send(new IEventHandle(
            cmd->GetResponseTarget(),
            cmd->GetFinalTarget(),
            ack.release(),
            0, // flags
            cmd->GetCookie()
        ));

        delete cmd;
        return;
    }

    TRcBuf localBuf = std::move(localBufOpt.value());

    // Extract RDMA region from local buffer
    // Get device index from QP context - default to 0 for now
    ssize_t deviceIndex = qp ? qp->GetCtx()->GetDeviceIndex() : 0;
    auto localRegionOpt = ExtractRdmaRegionFromRcBuf(localBuf, deviceIndex);
    if (!localRegionOpt) {
        LOG_ERROR_S(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaCqCommandHandler: Failed to extract RDMA region from local buffer");

        auto ack = std::make_unique<TEvRdmaWriteAck>(
            cmd->GetCookie(),
            false,
            "Failed to extract RDMA region from local buffer");
        actorSystem->Send(new IEventHandle(
            cmd->GetResponseTarget(),
            cmd->GetFinalTarget(),
            ack.release(),
            0, // flags
            cmd->GetCookie()
        ));

        delete cmd;
        return;
    }

    auto& localRegion = *localRegionOpt;

    // Set span event before issuing RDMA_READ
    span.Event("RDMA.PostRead");

    // Post RDMA_READ to fetch data from source
    // Wrap span in shared_ptr since std::function requires copyable types
    auto spanPtr = std::make_shared<NWilson::TSpan>(std::move(span));
    auto readCallback = [cmd, localBuf = std::move(localBuf), qp, cq, spanPtr](
        TActorSystem* as, NInterconnect::NRdma::TEvRdmaIoDone* ioDone) mutable {
        Y_UNUSED(qp);
        Y_UNUSED(cq);

        if (!ioDone->IsSuccess()) {
            LOG_ERROR_S(*as, NActorsServices::INTERCONNECT,
                "RdmaCqCommandHandler: RDMA_READ failed for cookie=" << cmd->GetCookie()
                << " error=" << ioDone->GetErrSource() << " code=" << ioDone->GetErrCode());

            TStringBuilder errMsg;
            errMsg << "RDMA_READ failed: " << ioDone->GetErrSource() << " code=" << ioDone->GetErrCode();

            spanPtr->EndError(TString(errMsg));

            auto ack = std::make_unique<TEvRdmaWriteAck>(
                cmd->GetCookie(),
                false,
                TString(errMsg));
            as->Send(new IEventHandle(
                cmd->GetResponseTarget(),
                cmd->GetFinalTarget(),
                ack.release(),
                0, // flags
                cmd->GetCookie()
            ));

            delete cmd;
            return;
        }

        LOG_DEBUG_S(*as, NActorsServices::INTERCONNECT,
            "RdmaCqCommandHandler: RDMA_READ completed successfully for cookie=" << cmd->GetCookie());

        spanPtr->Event("RDMA.ReadComplete");

        // Reconstruct embedded event from ApplicationPayload using protobuf
        ui32 embeddedEventType = cmd->GetEmbeddedEventType();
        const TString& appPayload = cmd->GetApplicationPayload();

        auto reconstructedEvent = CreateEventByType(embeddedEventType);
        if (!reconstructedEvent) {
            LOG_ERROR_S(*as, NActorsServices::INTERCONNECT,
                "RdmaCqCommandHandler: Failed to create event type " << embeddedEventType
                << " for cookie=" << cmd->GetCookie());

            auto ack = std::make_unique<TEvRdmaWriteAck>(
                cmd->GetCookie(),
                -1,
                "Failed to create event from factory");
            as->Send(new IEventHandle(
                cmd->GetResponseTarget(),
                cmd->GetFinalTarget(),
                ack.release(),
                0, // flags
                cmd->GetCookie()
            ));

            delete cmd;
            return;
        }

        // Deserialize the event from protobuf
        auto* rdmaPassable = AsRdmaPassable(reconstructedEvent.get());
        if (!rdmaPassable) {
            LOG_ERROR_S(*as, NActorsServices::INTERCONNECT,
                "RdmaCqCommandHandler: Event type " << embeddedEventType
                << " doesn't implement IRdmaPassable for cookie=" << cmd->GetCookie());

            auto ack = std::make_unique<TEvRdmaWriteAck>(
                cmd->GetCookie(),
                -1,
                "Event doesn't implement IRdmaPassable");
            as->Send(new IEventHandle(
                cmd->GetResponseTarget(),
                cmd->GetFinalTarget(),
                ack.release(),
                0, // flags
                cmd->GetCookie()
            ));

            delete cmd;
            return;
        }

        spanPtr->Event("RDMA.ReconstructedEvent");

        if (!rdmaPassable->DeserializeMetadata(appPayload)) {
            LOG_ERROR_S(*as, NActorsServices::INTERCONNECT,
                "RdmaCqCommandHandler: Failed to deserialize event metadata for cookie=" << cmd->GetCookie());

            auto ack = std::make_unique<TEvRdmaWriteAck>(
                cmd->GetCookie(),
                -1,
                "Failed to deserialize event metadata");
            as->Send(new IEventHandle(
                cmd->GetResponseTarget(),
                cmd->GetFinalTarget(),
                ack.release(),
                0, // flags
                cmd->GetCookie()
            ));

            delete cmd;
            return;
        }

        spanPtr->Event("RDMA.DeserializedEmbeddedEvent");

        // Attach payload data to reconstructed event using IRdmaPassable interface
        LOG_DEBUG_S(*as, NActorsServices::INTERCONNECT,
            "RdmaCqCommandHandler: Attaching RDMA payload of size " << localBuf.size()
            << " to reconstructed event for cookie=" << cmd->GetCookie());

        // Convert TRcBuf to TRope and attach
        TRope dataRope(TString(localBuf.data(), localBuf.size()));
        spanPtr->Event("RDMA.AllocatedRopeForPayload");

        rdmaPassable->SetRdmaPayload(std::move(dataRope));

        // Finish span before sending to local actor
        spanPtr->EndOk();

        // Send reconstructed event to final target
        // The target actor receives TEvDDiskWriteRequest as if it was transferred without RDMA
        as->Send(new IEventHandle(
            cmd->GetFinalTarget(),
            cmd->GetResponseTarget(),
            reconstructedEvent.release(),
            0, // flags
            cmd->GetCookie(),
            nullptr,
            spanPtr->GetTraceId()
        ));

        LOG_DEBUG_S(*as, NActorsServices::INTERCONNECT,
            "RdmaCqCommandHandler: Sent reconstructed event to " << cmd->GetFinalTarget().ToString()
            << " for cookie=" << cmd->GetCookie());

        // Send acknowledgment back to source
        auto ack = std::make_unique<TEvRdmaWriteAck>(
            cmd->GetCookie(),
            0,  // success
            "");
        as->Send(new IEventHandle(
            cmd->GetResponseTarget(),
            cmd->GetFinalTarget(),
            ack.release(),
            0, // flags
            cmd->GetCookie()
        ));

        delete cmd;
    };

    // Post RDMA_READ using the API
    // Note: We need the source actor ID to get the connection
    // It should be in cmd->GetResponseTarget() (which is where we send the ack, i.e., the source)
    bool posted = PostRdmaRead(
        actorSystem,
        cmd->GetResponseTarget(),  // Source actor ID (on remote node)
        reinterpret_cast<void*>(cmd->GetSourceRegion().Address),  // Remote source address
        cmd->GetSourceRegion().Rkey,  // Remote source rkey
        reinterpret_cast<void*>(localRegion.Address),  // Local dest address
        localRegion.Lkey,  // Local dest lkey
        cmd->GetSourceRegion().Size,  // Size to read
        readCallback
    );

    if (!posted) {
        LOG_ERROR_S(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaCqCommandHandler: Failed to post RDMA_READ for cookie=" << cmd->GetCookie());

        auto ack = std::make_unique<TEvRdmaWriteAck>(
            cmd->GetCookie(),
            false,
            "Failed to post RDMA_READ");
        actorSystem->Send(new IEventHandle(
            cmd->GetResponseTarget(),
            cmd->GetFinalTarget(),
            ack.release(),
            0, // flags
            cmd->GetCookie()
        ));

        delete cmd;
    }
}

void TRdmaCqCommandHandler::HandleReadCommand(
    TActorSystem* actorSystem,
    TEvRdmaReadCommand* cmd,
    std::shared_ptr<NInterconnect::NRdma::TQueuePair> qp,
    NInterconnect::NRdma::ICq::TPtr cq,
    NWilson::TSpan span)
{
    Y_UNUSED(qp);
    Y_UNUSED(cq);

    LOG_INFO_S(*actorSystem, NActorsServices::INTERCONNECT,
        "RdmaCqCommandHandler::HandleReadCommand cookie=" << cmd->GetCookie()
        << " destRegion={addr=" << cmd->GetDestinationRegion().Address
        << " rkey=" << cmd->GetDestinationRegion().Rkey
        << " size=" << cmd->GetDestinationRegion().Size << "}"
        << " autoExecute=" << cmd->GetAutoExecuteRdmaWrite()
        << " finalTarget=" << cmd->GetFinalTarget().ToString()
        << " responseTarget=" << cmd->GetResponseTarget().ToString());

    // Reconstruct the original request event from ApplicationPayload
    // ApplicationPayload contains serialized request metadata
    TRdmaEventMetadata meta;
    if (!TRdmaEventMetadata::Deserialize(cmd->GetApplicationPayload(), meta)) {
        LOG_ERROR_S(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaCqCommandHandler::HandleReadCommand: Failed to deserialize event metadata for cookie=" << cmd->GetCookie());

        auto ack = std::make_unique<TEvRdmaReadAck>(
            cmd->GetCookie(),
            -1,
            "Failed to deserialize event metadata");
        actorSystem->Send(new IEventHandle(
            cmd->GetResponseTarget(),
            cmd->GetFinalTarget(),
            ack.release(),
            0,
            cmd->GetCookie()
        ));

        delete cmd;
        return;
    }

    // Reconstruct the request event (e.g., TEvDDiskReadRequest)
    auto requestEvent = ReconstructEvent(meta, cmd->GetApplicationPayload());
    if (!requestEvent) {
        LOG_ERROR_S(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaCqCommandHandler::HandleReadCommand: Failed to reconstruct event for cookie=" << cmd->GetCookie());

        auto ack = std::make_unique<TEvRdmaReadAck>(
            cmd->GetCookie(),
            -1,
            "Failed to reconstruct event");
        actorSystem->Send(new IEventHandle(
            cmd->GetResponseTarget(),
            cmd->GetFinalTarget(),
            ack.release(),
            0,
            cmd->GetCookie()
        ));

        delete cmd;
        return;
    }

    // Create a wrapper event that includes destination region info
    // We'll embed this info in a local event that DDisk can use for completion
    // For now, create a helper actor that will handle the request/response cycle

    // Actually, simpler approach: Send request to DDisk and register a response handler
    // that will call RdmaAwareReadCompletion when response arrives

    // Store destination region info for later use in completion
    // We'll create a special wrapper that DDisk helper can use

    // For now, let's just send the reconstructed request to the target
    // and assume the target will somehow know to call RdmaAwareReadCompletion
    // (This needs coordination with DDisk actor implementation)

    LOG_DEBUG_S(*actorSystem, NActorsServices::INTERCONNECT,
        "RdmaCqCommandHandler::HandleReadCommand: Sending reconstructed request to "
        << cmd->GetFinalTarget().ToString());

    // TODO: We need a way to pass destination region info to the DDisk actor
    // For now, send the reconstructed event directly
    // In a complete implementation, we'd need a wrapper or context that includes:
    // - Original request event
    // - Destination region (destRegion.Address, destRegion.Rkey, destRegion.Size)
    // - Source actor (cmd->GetResponseTarget())
    // - Cookie (cmd->GetCookie())

    // Finish span before sending to local actor
    span.EndOk();

    actorSystem->Send(new IEventHandle(
        cmd->GetFinalTarget(),
        cmd->GetResponseTarget(),
        requestEvent.release(),
        0,
        cmd->GetCookie(),
        nullptr,
        span.GetTraceId()
    ));

    // TODO: Implement RDMA read context registry for read operations
    LOG_INFO_S(*actorSystem, NActorsServices::INTERCONNECT,
        "RdmaCqCommandHandler::HandleReadCommand: Forwarded request to DDisk, cookie=" << cmd->GetCookie()
        << ", destAddr=" << cmd->GetDestinationRegion().Address
        << ", destRkey=" << cmd->GetDestinationRegion().Rkey
        << ", destSize=" << cmd->GetDestinationRegion().Size);

    delete cmd;
}

} // namespace NActors
