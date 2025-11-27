#pragma once

#include "rdma_data_transfer_events.h"
#include "rdma_event_serializer.h"
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/interconnect/rdma/rdma.h>
#include <memory>

namespace NActors {

////////////////////////////////////////////////////////////////////////////////
// RDMA RECV Handler
//
// Handles incoming RDMA RECV completions in CQ polling loop.
// This is called from the CQ thread when IBV_WC_RECV completion is polled.
//
// Flow:
// 1. CQ polls work completions
// 2. For IBV_WC_RECV operations, deserialize the received buffer
// 3. Check if it's a RDMA command (TEvRdmaWriteCommand/TEvRdmaReadCommand)
// 4. Route to appropriate handler:
//    - For TEvRdmaWriteCommand: Call TRdmaCqCommandHandler::HandleWriteCommand
//    - For TEvRdmaReadCommand: Call TRdmaCqCommandHandler::HandleReadCommand
//    - For regular events: Reconstruct and send to target actor
////////////////////////////////////////////////////////////////////////////////

class TRdmaRecvHandler {
public:
    // Handle incoming RECV completion
    // This is called from CQ polling thread
    static void HandleRecvCompletion(
        TActorSystem* actorSystem,
        const ibv_wc* wc,
        void* recvBuffer,
        size_t recvSize,
        std::shared_ptr<NInterconnect::NRdma::TQueuePair> qp,
        NInterconnect::NRdma::ICq::TPtr cq);

private:
    // Deserialize and dispatch based on event type
    static void DispatchReceivedData(
        TActorSystem* actorSystem,
        const char* data,
        size_t size,
        std::shared_ptr<NInterconnect::NRdma::TQueuePair> qp,
        NInterconnect::NRdma::ICq::TPtr cq);
};

////////////////////////////////////////////////////////////////////////////////
// Implementation Notes
//
// To integrate this into the CQ polling loop (rdma.cpp):
//
// 1. In TSimpleCqBase::HandleWc(), check work completion opcode:
//    if (wc->opcode == IBV_WC_RECV) {
//        // Get the RECV buffer associated with this work request
//        void* recvBuffer = GetRecvBuffer(wc->wr_id);
//        size_t recvSize = wc->byte_len;
//
//        TRdmaRecvHandler::HandleRecvCompletion(
//            As, wc, recvBuffer, recvSize, Qp, Cq);
//
//        // Repost RECV work request for next incoming message
//        RepostRecv(wc->wr_id);
//    }
//
// 2. Need to pre-post RECV work requests in session initialization
//    - Allocate recv buffers from RDMA memory pool
//    - Post IBV_WR_RECV work requests
//    - Track mapping of wr_id to recv buffer
//
// 3. Session needs to maintain pool of RECV buffers
//    - Pre-allocate N buffers (e.g., 64)
//    - Post them all as RECV WRs
//    - When one completes, process and repost
////////////////////////////////////////////////////////////////////////////////

} // namespace NActors
