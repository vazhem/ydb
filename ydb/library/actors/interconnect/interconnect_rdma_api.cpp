#include "interconnect_rdma_api.h"
#include "interconnect_tcp_proxy.h"
#include "interconnect_tcp_session.h"
#include "rdma/rdma.h"
#include "rdma/mem_pool.h"
#include "rdma_event_serializer.h"
#include "rdma_connection_registry.h"
#include "rdma_memory_region.h"
#include "rdma_data_transfer_events.h"
#include "rdma_event_base.h"
#include "rdma_cq_command_handler.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/util/rc_buf.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <util/system/mutex.h>
#include <util/generic/hash.h>
#include <cstring>

extern "C" {
#include <infiniband/verbs.h>
}

namespace NActors {

////////////////////////////////////////////////////////////////////////////////
// Helper: Convert ibverbs error codes to human-readable strings
////////////////////////////////////////////////////////////////////////////////

namespace {

const char* IbvWcStatusStr(int status) {
    switch (status) {
        case IBV_WC_SUCCESS: return "SUCCESS";
        case IBV_WC_LOC_LEN_ERR: return "LOC_LEN_ERR(Local Length Error)";
        case IBV_WC_LOC_QP_OP_ERR: return "LOC_QP_OP_ERR(Local QP Operation Error)";
        case IBV_WC_LOC_EEC_OP_ERR: return "LOC_EEC_OP_ERR(Local EE Context Operation Error)";
        case IBV_WC_LOC_PROT_ERR: return "LOC_PROT_ERR(Local Protection Error)";
        case IBV_WC_WR_FLUSH_ERR: return "WR_FLUSH_ERR(Work Request Flushed Error)";
        case IBV_WC_MW_BIND_ERR: return "MW_BIND_ERR(Memory Window Bind Error)";
        case IBV_WC_BAD_RESP_ERR: return "BAD_RESP_ERR(Bad Response Error)";
        case IBV_WC_LOC_ACCESS_ERR: return "LOC_ACCESS_ERR(Local Access Error)";
        case IBV_WC_REM_INV_REQ_ERR: return "REM_INV_REQ_ERR(Remote Invalid Request Error)";
        case IBV_WC_REM_ACCESS_ERR: return "REM_ACCESS_ERR(Remote Access Error)";
        case IBV_WC_REM_OP_ERR: return "REM_OP_ERR(Remote Operation Error)";
        case IBV_WC_RETRY_EXC_ERR: return "RETRY_EXC_ERR(Transport Retry Counter Exceeded)";
        case IBV_WC_RNR_RETRY_EXC_ERR: return "RNR_RETRY_EXC_ERR(RNR Retry Counter Exceeded)";
        case IBV_WC_LOC_RDD_VIOL_ERR: return "LOC_RDD_VIOL_ERR(Local RDD Violation Error)";
        case IBV_WC_REM_INV_RD_REQ_ERR: return "REM_INV_RD_REQ_ERR(Remote Invalid RD Request)";
        case IBV_WC_REM_ABORT_ERR: return "REM_ABORT_ERR(Remote Aborted Error)";
        case IBV_WC_INV_EECN_ERR: return "INV_EECN_ERR(Invalid EE Context Number)";
        case IBV_WC_INV_EEC_STATE_ERR: return "INV_EEC_STATE_ERR(Invalid EE Context State)";
        case IBV_WC_FATAL_ERR: return "FATAL_ERR(Fatal Error)";
        case IBV_WC_RESP_TIMEOUT_ERR: return "RESP_TIMEOUT_ERR(Response Timeout Error)";
        case IBV_WC_GENERAL_ERR: return "GENERAL_ERR(General Error)";
        default: return "UNKNOWN_ERROR";
    }
}

const char* IbvQpStateStr(int state) {
    switch (state) {
        case IBV_QPS_RESET: return "RESET";
        case IBV_QPS_INIT: return "INIT";
        case IBV_QPS_RTR: return "RTR(Ready to Receive)";
        case IBV_QPS_RTS: return "RTS(Ready to Send)";
        case IBV_QPS_SQD: return "SQD(Send Queue Drained)";
        case IBV_QPS_SQE: return "SQE(Send Queue Error)";
        case IBV_QPS_ERR: return "ERR(Error)";
        default: return "UNKNOWN";
    }
}

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////
// Helper: Get Session by Actor ID
////////////////////////////////////////////////////////////////////////////////

namespace {

// Get RDMA connection info for a node using the global registry
std::optional<TRdmaConnectionInfo> GetConnectionInfo(ui32 nodeId) {
    return TRdmaConnectionRegistry::Instance().Get(nodeId);
}

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////
// Public API Implementation
////////////////////////////////////////////////////////////////////////////////

std::optional<TRdmaConnectionHandle> GetRdmaConnection(
    TActorSystem* actorSystem,
    TActorId targetActor)
{
    // Extract node ID from actor ID
    const ui32 targetNodeId = targetActor.NodeId();
    const ui32 localNodeId = actorSystem->NodeId;

    LOG_DEBUG(*actorSystem, NActorsServices::INTERCONNECT,
        "GetRdmaConnection: Requested connection from node %u to %s (node %u)",
        localNodeId, targetActor.ToString().c_str(), targetNodeId);

    // Check if target is local
    if (targetNodeId == localNodeId || targetNodeId == 0) {
        LOG_DEBUG(*actorSystem, NActorsServices::INTERCONNECT,
            "GetRdmaConnection: Target %s is local or invalid node ID %u, no RDMA needed",
            targetActor.ToString().c_str(), targetNodeId);
        return std::nullopt;  // Local actor, no RDMA needed
    }

    // Get connection info for target node
    auto connInfoOpt = GetConnectionInfo(targetNodeId);

    if (!connInfoOpt) {
        LOG_DEBUG(*actorSystem, NActorsServices::INTERCONNECT,
            "GetRdmaConnection: No RDMA connection info available for node %u", targetNodeId);
        return std::nullopt;  // No RDMA connection available
    }

    if (!connInfoOpt->IsValid()) {
        LOG_DEBUG(*actorSystem, NActorsServices::INTERCONNECT,
            "GetRdmaConnection: RDMA connection info for node %u is invalid", targetNodeId);
        return std::nullopt;  // No RDMA connection available
    }

    auto& connInfo = *connInfoOpt;

    // Get QP state for logging
    auto qpState = connInfo.Qp->GetState(false);
    int stateValue = -1;
    const char* stateStr = "unknown";
    bool isError = false;

    if (std::holds_alternative<NInterconnect::NRdma::TQueuePair::TQpS>(qpState)) {
        stateValue = std::get<NInterconnect::NRdma::TQueuePair::TQpS>(qpState).State;
        stateStr = IbvQpStateStr(stateValue);
    } else if (std::holds_alternative<NInterconnect::NRdma::TQueuePair::TQpErr>(qpState)) {
        stateValue = std::get<NInterconnect::NRdma::TQueuePair::TQpErr>(qpState).Err;
        stateStr = "ERROR";
        isError = true;
    }

    LOG_DEBUG(*actorSystem, NActorsServices::INTERCONNECT,
        "GetRdmaConnection: Found %sRDMA connection to node %u (device=%u, qp=%p, qpNum=%u, cq=%p, qp_state=%s/%d)",
        isError ? "ERROR " : "valid ",
        targetNodeId, connInfo.RdmaDeviceIndex,
        static_cast<void*>(connInfo.Qp.get()), connInfo.Qp->GetQpNum(),
        static_cast<void*>(connInfo.Cq.get()), stateStr, stateValue);

    // Create and return connection handle
    return TRdmaConnectionHandle(
        connInfo.Qp,
        connInfo.Cq,
        connInfo.PeerQpNum,
        connInfo.PeerGid,
        connInfo.RdmaDeviceIndex
    );
}

bool RdmaSend(
    TActorSystem* actorSystem,
    TActorId srcActor,
    TActorId tgtActor,
    IEventBase* event,
    ui64 cookie,
    NWilson::TTraceId traceId)
{
    NWilson::TSpan span(
                NKikimr::TWilson::BlobStorage,
                std::move(traceId.Clone()),
                "RDMA.RdmaSend",
                NWilson::EFlags::NONE,
                actorSystem
            );

    LOG_INFO(*actorSystem, NActorsServices::INTERCONNECT,
        "=== RdmaSend CALLED === from %s to %s, cookie=%lu, eventType=%u",
        srcActor.ToString().c_str(), tgtActor.ToString().c_str(), cookie, event->Type());

    // Get RDMA connection
    auto connOpt = GetRdmaConnection(actorSystem, tgtActor);
    if (!connOpt) {
        LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaSend: No RDMA connection found to %s (nodeId=%u)",
            tgtActor.ToString().c_str(), tgtActor.NodeId());
        return false;
    }

    auto& conn = *connOpt;

    LOG_INFO(*actorSystem, NActorsServices::INTERCONNECT,
        "RdmaSend: Found RDMA connection - localQp=%u, peerQpNum=%u, device=%zd",
        conn.Qp->GetQpNum(), conn.PeerQpNum, conn.RdmaDeviceIndex);

    if (!conn.Qp || !conn.Cq) {
        LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaSend: Invalid connection handle for %s", tgtActor.ToString().c_str());
        return false;
    }

    // Serialize event for RDMA transfer using actor system's serialization approach
    // Format: [TRdmaMessageHeader][Serialized Event Data]
    ui32 eventType = event->Type();

    LOG_DEBUG(*actorSystem, NActorsServices::INTERCONNECT,
        "RdmaSend: Attempting to serialize event type %u", eventType);

    // Use TEventPB serialization approach (SerializeToArcadiaStream)
    // This works for all protobuf-based events (TEventPB), not just RDMA command wrappers
    TAllocChunkSerializer serializer;
    if (!event->SerializeToArcadiaStream(&serializer)) {
        LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaSend: Failed to serialize event type %u (event may not be TEventPB-based)", eventType);
        return false;
    }

    span.Event("RDMA.SerializedEvent");

    // Get serialized data
    auto serializedData = serializer.Release(event->CreateSerializationInfo());
    TString payload = serializedData->GetString();

    if (payload.empty()) {
        LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaSend: Serialization produced empty data for eventType=%u", eventType);
        return false;
    }

    LOG_DEBUG(*actorSystem, NActorsServices::INTERCONNECT,
        "RdmaSend: Serialized event type %u to %zu bytes using TEventPB serialization",
        eventType, payload.size());

    // Prepend TRdmaMessageHeader (similar to TEventDescr2 with TraceId)
    TRdmaMessageHeader header(eventType, 0, tgtActor, srcActor, cookie, span.GetTraceId());

    // Create complete message: header + payload
    TString metadata;
    metadata.reserve(sizeof(header) + payload.size());
    metadata.append(reinterpret_cast<const char*>(&header), sizeof(header));
    metadata.append(payload);

    LOG_DEBUG(*actorSystem, NActorsServices::INTERCONNECT,
        "RdmaSend: Complete message size: %zu bytes (header=%zu + payload=%zu), sender=%s, recipient=%s",
        metadata.size(), sizeof(header), payload.size(),
        srcActor.ToString().c_str(), tgtActor.ToString().c_str());

    // Allocate RDMA buffer for metadata
    auto* allocator = actorSystem->GetRcBufAllocator();
    if (!allocator) {
        LOG_WARN(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaSend: No RDMA allocator available");
        return false;
    }

    std::optional<TRcBuf> rdmaBufOpt = allocator->AllocRcBuf(metadata.size(), 0, 0);
    if (!rdmaBufOpt.has_value()) {
        LOG_WARN(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaSend: Failed to allocate RDMA buffer of size %zu", metadata.size());
        return false;
    }

    span.Event("RDMA.AllocatedRdmaBuffer");

    TRcBuf rdmaBuf = std::move(rdmaBufOpt.value());

    // Copy metadata to RDMA buffer
    char* dest = const_cast<char*>(rdmaBuf.data());
    memcpy(dest, metadata.data(), metadata.size());

    span.Event("RDMA.CopiedToRdmaBuffer");

    // Extract memory region info using existing functions
    auto regionOpt = ExtractRdmaRegionFromRcBuf(rdmaBuf, conn.RdmaDeviceIndex);
    if (!regionOpt) {
        LOG_WARN(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaSend: Failed to extract RDMA region from buffer");
        return false;
    }

    span.Event("RDMA.ExtractRdmaRegionFromRcBuf");

    auto& region = *regionOpt;

    // Allocate work request with callback
    auto callback = [srcActor, tgtActor, metadata = std::move(metadata), rdmaBuf = std::move(rdmaBuf),
                     regionSize = region.Size, regionAddr = region.Address, regionLkey = region.Lkey](
        TActorSystem* as, NInterconnect::NRdma::TEvRdmaIoDone* ioDone) mutable {
        Y_UNUSED(metadata);  // Keep metadata alive until completion
        Y_UNUSED(rdmaBuf);   // Keep buffer alive until completion

        if (ioDone->IsSuccess()) {
            LOG_DEBUG(*as, NActorsServices::INTERCONNECT,
                "RdmaSend: SEND completed successfully from %s to %s (size=%u, addr=%p, lkey=%u)",
                srcActor.ToString().c_str(), tgtActor.ToString().c_str(),
                regionSize, reinterpret_cast<void*>(regionAddr), regionLkey);
        } else {
            int errCode = ioDone->GetErrCode();
            const char* errStr = (errCode >= 0) ? IbvWcStatusStr(errCode) : "N/A";
            LOG_ERROR(*as, NActorsServices::INTERCONNECT,
                "RdmaSend: SEND failed: %s code=%d (%s) from %s to %s (size=%u, addr=%p, lkey=%u)",
                ioDone->GetErrSource().data(), errCode, errStr,
                srcActor.ToString().c_str(), tgtActor.ToString().c_str(),
                regionSize, reinterpret_cast<void*>(regionAddr), regionLkey);
        }
    };

    auto allocResult = conn.Cq->AllocWr(std::move(callback));
    if (NInterconnect::NRdma::ICq::IsWrBusy(allocResult)) {
        LOG_WARN(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaSend: CQ is busy");
        return false;
    }
    if (NInterconnect::NRdma::ICq::IsWrErr(allocResult)) {
        LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaSend: CQ error");
        return false;
    }

    span.Event("RDMA.AllocWr");

    auto* wr = std::get<NInterconnect::NRdma::ICq::IWr*>(allocResult);
    ui64 wrId = wr->GetId();

    // Create SEND work request using low-level ibverbs API
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = region.Address;
    sge.length = region.Size;
    sge.lkey = region.Lkey;

    struct ibv_send_wr sendWr, *bad_wr = nullptr;
    memset(&sendWr, 0, sizeof(sendWr));
    sendWr.wr_id = wrId;
    sendWr.sg_list = &sge;
    sendWr.num_sge = 1;
    sendWr.opcode = IBV_WR_SEND;
    sendWr.send_flags = IBV_SEND_SIGNALED;  // Signal completion

    span.Event("RDMA.FilledWr");

    // Use inline send for small messages (< 256 bytes) to avoid memory registration overhead
    // Most control messages like TEvDDiskWriteRequest without payload fit in 256 bytes
    if (region.Size <= 256) {
        sendWr.send_flags |= IBV_SEND_INLINE;
    }

    LOG_INFO(*actorSystem, NActorsServices::INTERCONNECT,
        "RdmaSend: *** POSTING SEND *** wr_id=%lu size=%u addr=%p lkey=%u %s from %s to %s (nodeId=%u, device=%u, peerQpNum=%u)",
        wrId, region.Size, reinterpret_cast<void*>(region.Address), region.Lkey,
        (sendWr.send_flags & IBV_SEND_INLINE) ? "[INLINE]" : "[REGISTERED]",
        srcActor.ToString().c_str(), tgtActor.ToString().c_str(),
        tgtActor.NodeId(), conn.RdmaDeviceIndex, conn.PeerQpNum);

    span.EndOk();
    // Post SEND work request
    int postResult = conn.Qp->PostSend(&sendWr, &bad_wr);
    if (postResult != 0) {
        LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaSend: Failed to post SEND work request: errno=%d (%s), wr_id=%lu, from %s to %s",
            postResult, strerror(postResult), wrId,
            srcActor.ToString().c_str(), tgtActor.ToString().c_str());

        // Check QP state after failure
        auto qpStateAfter = conn.Qp->GetState(true);
        if (std::holds_alternative<NInterconnect::NRdma::TQueuePair::TQpS>(qpStateAfter)) {
            int state = std::get<NInterconnect::NRdma::TQueuePair::TQpS>(qpStateAfter).State;
            LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
                "RdmaSend: QP state after PostSend failure: %s (%d)", IbvQpStateStr(state), state);
        } else if (std::holds_alternative<NInterconnect::NRdma::TQueuePair::TQpErr>(qpStateAfter)) {
            int err = std::get<NInterconnect::NRdma::TQueuePair::TQpErr>(qpStateAfter).Err;
            LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
                "RdmaSend: QP is in error state after PostSend failure: err=%d", err);
        }

        wr->Release();
        return false;
    }

    LOG_DEBUG(*actorSystem, NActorsServices::INTERCONNECT,
        "RdmaSend: Successfully posted SEND of %u bytes to %s via RDMA (wr_id=%lu)",
        region.Size, tgtActor.ToString().c_str(), wrId);

    // Note: event is now managed by the callback and will be freed when done
    delete event;

    return true;
}

bool PostRdmaRead(
    TActorSystem* actorSystem,
    TActorId sourceActorId,
    void* sourceAddr,
    ui32 sourceRkey,
    void* destAddr,
    ui32 destLkey,
    ui32 destSize,
    std::function<void(TActorSystem* as, NInterconnect::NRdma::TEvRdmaIoDone*)> callback,
    NWilson::TTraceId traceId)
{
    // Note: traceId is accepted for API consistency but not used in low-level RDMA operations
    Y_UNUSED(traceId);
    // Get RDMA connection
    auto connOpt = GetRdmaConnection(actorSystem, sourceActorId);
    if (!connOpt) {
        LOG_WARN(*actorSystem, NActorsServices::INTERCONNECT,
            "PostRdmaRead: No RDMA connection to %s", sourceActorId.ToString().c_str());
        return false;  // No RDMA connection
    }

    auto& conn = *connOpt;

    if (!conn.Qp || !conn.Cq) {
        LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
            "PostRdmaRead: Invalid connection handle for %s", sourceActorId.ToString().c_str());
        return false;
    }

    // Create IbVerbs builder
    auto builder = NInterconnect::NRdma::CreateIbVerbsBuilder(1);
    if (!builder) {
        LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
            "PostRdmaRead: Failed to create IbVerbsBuilder");
        return false;
    }

    // Add READ verb
    builder->AddReadVerb(
        destAddr,       // local destination address
        destLkey,       // local destination key
        sourceAddr,     // remote source address
        sourceRkey,     // remote source key
        destSize,       // size to read
        std::move(callback));

    // Post work request batch
    auto postResult = conn.Cq->DoWrBatchAsync(conn.Qp, std::move(builder));
    if (postResult.has_value()) {
        LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
            "PostRdmaRead: Failed to post work request batch");
        return false;
    }

    LOG_DEBUG(*actorSystem, NActorsServices::INTERCONNECT,
        "PostRdmaRead: Posted RDMA Read of %u bytes from remote %p to local %p",
        destSize, sourceAddr, destAddr);

    return true;
}

bool PostRdmaWrite(
    TActorSystem* actorSystem,
    TActorId targetActorId,
    void* sourceAddr,
    ui32 sourceLkey,
    ui32 sourceSize,
    void* destAddr,
    ui32 destRkey,
    std::function<void(TActorSystem* as, NInterconnect::NRdma::TEvRdmaIoDone*)> callback,
    NWilson::TTraceId traceId)
{
    // Note: traceId is accepted for API consistency but not used in low-level RDMA operations
    Y_UNUSED(traceId);
    // Get RDMA connection
    auto connOpt = GetRdmaConnection(actorSystem, targetActorId);
    if (!connOpt) {
        LOG_WARN(*actorSystem, NActorsServices::INTERCONNECT,
            "PostRdmaWrite: No RDMA connection to %s", targetActorId.ToString().c_str());
        return false;  // No RDMA connection
    }

    auto& conn = *connOpt;

    if (!conn.Qp || !conn.Cq) {
        LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
            "PostRdmaWrite: Invalid connection handle for %s", targetActorId.ToString().c_str());
        return false;
    }

    // Allocate work request with callback
    auto allocResult = conn.Cq->AllocWr(std::move(callback));
    if (NInterconnect::NRdma::ICq::IsWrBusy(allocResult)) {
        LOG_WARN(*actorSystem, NActorsServices::INTERCONNECT,
            "PostRdmaWrite: CQ is busy");
        return false;
    }
    if (NInterconnect::NRdma::ICq::IsWrErr(allocResult)) {
        LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
            "PostRdmaWrite: CQ error");
        return false;
    }

    auto* wr = std::get<NInterconnect::NRdma::ICq::IWr*>(allocResult);
    ui64 wrId = wr->GetId();

    // Check QP state before posting write
    auto qpState = conn.Qp->GetState(true);
    if (std::holds_alternative<NInterconnect::NRdma::TQueuePair::TQpErr>(qpState)) {
        int errCode = std::get<NInterconnect::NRdma::TQueuePair::TQpErr>(qpState).Err;
        LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
            "PostRdmaWrite: QP is in error state (err=%d) before posting write to %s",
            errCode, targetActorId.ToString().c_str());
        wr->Release();
        return false;
    }

    int qpStateValue = -1;
    if (std::holds_alternative<NInterconnect::NRdma::TQueuePair::TQpS>(qpState)) {
        qpStateValue = std::get<NInterconnect::NRdma::TQueuePair::TQpS>(qpState).State;
        if (qpStateValue != IBV_QPS_RTS) {
            LOG_WARN(*actorSystem, NActorsServices::INTERCONNECT,
                "PostRdmaWrite: QP is not in RTS state (current: %s), write may fail",
                IbvQpStateStr(qpStateValue));
        }
    }

    // Create RDMA WRITE work request using low-level ibverbs API
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = reinterpret_cast<ui64>(sourceAddr);
    sge.length = sourceSize;
    sge.lkey = sourceLkey;

    struct ibv_send_wr writeWr, *bad_wr = nullptr;
    memset(&writeWr, 0, sizeof(writeWr));
    writeWr.wr_id = wrId;
    writeWr.sg_list = &sge;
    writeWr.num_sge = 1;
    writeWr.opcode = IBV_WR_RDMA_WRITE;
    writeWr.send_flags = IBV_SEND_SIGNALED;  // Signal completion
    writeWr.wr.rdma.remote_addr = reinterpret_cast<ui64>(destAddr);
    writeWr.wr.rdma.rkey = destRkey;

    LOG_DEBUG(*actorSystem, NActorsServices::INTERCONNECT,
        "PostRdmaWrite: Posting RDMA WRITE wr_id=%lu size=%u local=%p/lkey=%u remote=%p/rkey=%u to %s",
        wrId, sourceSize, sourceAddr, sourceLkey, destAddr, destRkey,
        targetActorId.ToString().c_str());

    // Post RDMA WRITE work request
    int postResult = conn.Qp->PostSend(&writeWr, &bad_wr);
    if (postResult != 0) {
        LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
            "PostRdmaWrite: Failed to post RDMA WRITE: errno=%d (%s), wr_id=%lu, to %s",
            postResult, strerror(postResult), wrId, targetActorId.ToString().c_str());

        // Check QP state after failure
        auto qpStateAfter = conn.Qp->GetState(true);
        if (std::holds_alternative<NInterconnect::NRdma::TQueuePair::TQpS>(qpStateAfter)) {
            int state = std::get<NInterconnect::NRdma::TQueuePair::TQpS>(qpStateAfter).State;
            LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
                "PostRdmaWrite: QP state after failure: %s (%d)", IbvQpStateStr(state), state);
        } else if (std::holds_alternative<NInterconnect::NRdma::TQueuePair::TQpErr>(qpStateAfter)) {
            int err = std::get<NInterconnect::NRdma::TQueuePair::TQpErr>(qpStateAfter).Err;
            LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
                "PostRdmaWrite: QP is in error state after failure: err=%d", err);
        }

        wr->Release();
        return false;
    }

    LOG_DEBUG(*actorSystem, NActorsServices::INTERCONNECT,
        "PostRdmaWrite: Successfully posted RDMA Write of %u bytes from local %p to remote %p (wr_id=%lu)",
        sourceSize, sourceAddr, destAddr, wrId);

    return true;
}

////////////////////////////////////////////////////////////////////////////////
// RDMA-Aware Read Implementation
// Note: Memory tracking is now handled at NBS partition layer, not here
////////////////////////////////////////////////////////////////////////////////

bool RdmaAwareRead(
    TActorSystem* actorSystem,
    TActorId sourceActor,
    TActorId targetActor,
    IEventBase* requestEvent,
    ui64 cookie,
    ui32 expectedResponseSize,
    NWilson::TTraceId traceId)
{
    LOG_INFO(*actorSystem, NActorsServices::INTERCONNECT,
        "RdmaAwareRead: cookie=%lu from %s to %s, expectedSize=%u",
        cookie, sourceActor.ToString().c_str(), targetActor.ToString().c_str(), expectedResponseSize);

    // Check that event implements IRdmaPassable
    auto* rdmaPassable = AsRdmaPassable(requestEvent);
    if (!rdmaPassable) {
        LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaAwareRead: Event does not implement IRdmaPassable");
        delete requestEvent;
        return false;
    }

    // Simply send the request event via RDMA
    // The event should already contain RDMA memory region info set by the caller (NBS partition layer)
    bool sent = RdmaSend(actorSystem, sourceActor, targetActor, requestEvent, cookie, std::move(traceId));

    if (!sent) {
        LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaAwareRead: Failed to send read request via RDMA");
        delete requestEvent;
        return false;
    }

    LOG_DEBUG(*actorSystem, NActorsServices::INTERCONNECT,
        "RdmaAwareRead: Sent read request via RDMA, cookie=%lu", cookie);

    return true;
}

bool RdmaAwareReadCompletion(
    TActorSystem* actorSystem,
    TActorId targetActor,
    TActorId sourceActor,
    IEventBase* responseEvent,
    ui64 cookie,
    ui64 destAddress,
    ui32 destRkey,
    ui32 destSize,
    NWilson::TTraceId traceId)
{
    LOG_INFO(*actorSystem, NActorsServices::INTERCONNECT,
        "RdmaAwareReadCompletion: cookie=%lu from %s to %s, dest=%p, rkey=%u, size=%u",
        cookie, targetActor.ToString().c_str(), sourceActor.ToString().c_str(),
        reinterpret_cast<void*>(destAddress), destRkey, destSize);

    // Check that event implements IRdmaPassable
    auto* rdmaPassable = AsRdmaPassable(responseEvent);
    if (!rdmaPassable) {
        LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaAwareReadCompletion: Event does not implement IRdmaPassable");
        delete responseEvent;
        return false;
    }

    // Get response payload
    TRope responsePayload = rdmaPassable->GetRdmaPayload();
    ui32 payloadSize = responsePayload.GetSize();

    if (payloadSize == 0) {
        LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaAwareReadCompletion: Response has no payload");
        delete responseEvent;
        return false;
    }

    if (payloadSize > destSize) {
        LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaAwareReadCompletion: Response payload (%u) exceeds destination size (%u)",
            payloadSize, destSize);
        delete responseEvent;
        return false;
    }

    // Allocate local RDMA buffer for response payload
    auto* allocator = actorSystem->GetRcBufAllocator();
    if (!allocator) {
        LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaAwareReadCompletion: No RDMA allocator available");
        delete responseEvent;
        return false;
    }

    std::optional<TRcBuf> localBufOpt = allocator->AllocRcBuf(payloadSize, 0, 0);
    if (!localBufOpt.has_value()) {
        LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaAwareReadCompletion: Failed to allocate local buffer of size %u", payloadSize);
        delete responseEvent;
        return false;
    }

    TRcBuf localBuf = std::move(localBufOpt.value());

    // Copy payload to local buffer
    char* dest = const_cast<char*>(localBuf.data());
    responsePayload.Begin().ExtractPlainDataAndAdvance(dest, payloadSize);

    // Extract RDMA region from local buffer
    ui32 sourceNodeId = sourceActor.NodeId();
    auto connInfoOpt = GetConnectionInfo(sourceNodeId);
    if (!connInfoOpt || !connInfoOpt->IsValid()) {
        LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaAwareReadCompletion: No RDMA connection to node %u", sourceNodeId);
        delete responseEvent;
        return false;
    }

    auto localRegionOpt = ExtractRdmaRegionFromRcBuf(localBuf, connInfoOpt->RdmaDeviceIndex);
    if (!localRegionOpt) {
        LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaAwareReadCompletion: Failed to extract RDMA region from local buffer");
        delete responseEvent;
        return false;
    }

    auto& localRegion = *localRegionOpt;

    // Perform RDMA WRITE to source memory
    // Setup callback to send completion notification after RDMA completes
    auto writeCallback = [responseEvent, localBuf = std::move(localBuf),
                          sourceActor, targetActor, cookie, payloadSize, traceId = std::move(traceId)](
        TActorSystem* as, NInterconnect::NRdma::TEvRdmaIoDone* ioDone) mutable {
        Y_UNUSED(localBuf);  // Keep buffer alive until write completes

        if (!ioDone->IsSuccess()) {
            int errCode = ioDone->GetErrCode();
            const char* errStr = (errCode >= 0) ? IbvWcStatusStr(errCode) : "N/A";
            LOG_ERROR_S(*as, NActorsServices::INTERCONNECT,
                "RdmaAwareReadCompletion: RDMA WRITE failed for cookie=" << cookie
                << " error=" << ioDone->GetErrSource() << " code=" << errCode << " (" << errStr << ")"
                << " from " << targetActor.ToString() << " to " << sourceActor.ToString()
                << " size=" << payloadSize);
            delete responseEvent;
            return;
        }

        LOG_DEBUG_S(*as, NActorsServices::INTERCONNECT,
            "RdmaAwareReadCompletion: RDMA WRITE completed successfully for cookie=" << cookie
            << " size=" << payloadSize);

        // Now send completion notification via RdmaWriteCompletion (which wraps RdmaSend)
        // Response event is sent to notify source that RDMA WRITE is complete
        bool sent = RdmaSend(as, targetActor, sourceActor, responseEvent, cookie, std::move(traceId));
        if (!sent) {
            LOG_ERROR_S(*as, NActorsServices::INTERCONNECT,
                "RdmaAwareReadCompletion: Failed to send completion notification for cookie=" << cookie);
            delete responseEvent;
        }
    };

    // Post RDMA WRITE
    bool posted = PostRdmaWrite(
        actorSystem,
        sourceActor,  // Target actor on remote node
        reinterpret_cast<void*>(localRegion.Address),  // Source address (local)
        localRegion.Lkey,  // Source lkey
        payloadSize,  // Size to write
        reinterpret_cast<void*>(destAddress),  // Dest address (remote)
        destRkey,  // Dest rkey
        std::move(writeCallback),
        {}  // traceId already passed in callback
    );

    if (!posted) {
        LOG_ERROR(*actorSystem, NActorsServices::INTERCONNECT,
            "RdmaAwareReadCompletion: Failed to post RDMA WRITE for cookie=%lu", cookie);
        delete responseEvent;
        return false;
    }

    LOG_DEBUG(*actorSystem, NActorsServices::INTERCONNECT,
        "RdmaAwareReadCompletion: Posted RDMA WRITE for cookie=%lu", cookie);

    return true;
}

// RdmaWriteCompletion - wrapper over RdmaSend for write completion notifications
// This provides API consistency with read flow (RdmaRead -> RdmaReadCompletion)
bool RdmaWriteCompletion(
    TActorSystem* actorSystem,
    TActorId srcActor,
    TActorId tgtActor,
    IEventBase* event,
    ui64 cookie,
    NWilson::TTraceId traceId)
{
    LOG_DEBUG(*actorSystem, NActorsServices::INTERCONNECT,
        "RdmaWriteCompletion: Sending completion notification from %s to %s, cookie=%lu",
        srcActor.ToString().c_str(), tgtActor.ToString().c_str(), cookie);

    // Simply use RdmaSend - this is a wrapper for API consistency
    return RdmaSend(actorSystem, srcActor, tgtActor, event, cookie, std::move(traceId));
}

// GetRdmaReadContext: No longer needed
// RDMA metadata is now passed via proto fields in DDiskReadRequest
// (RdmaResponseAddr, RdmaResponseRkey)
bool GetRdmaReadContext(
    ui64 cookie,
    ui64& destAddress,
    ui32& destRkey,
    ui32& destSize,
    TActorId& sourceActor,
    TActorId& targetActor)
{
    Y_UNUSED(cookie);
    Y_UNUSED(destAddress);
    Y_UNUSED(destRkey);
    Y_UNUSED(destSize);
    Y_UNUSED(sourceActor);
    Y_UNUSED(targetActor);

    // This function is deprecated - RDMA metadata is now in proto fields
    return false;
}

} // namespace NActors
