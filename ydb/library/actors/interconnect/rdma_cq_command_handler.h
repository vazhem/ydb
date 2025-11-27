#pragma once

#include "rdma_data_transfer_events.h"
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/interconnect/rdma/rdma.h>
#include <ydb/library/actors/wilson/wilson_trace.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <memory>

namespace NActors {

////////////////////////////////////////////////////////////////////////////////
// RDMA CQ Command Handler
//
// Handles incoming RDMA commands received on CQ with auto-execution:
// - TEvRdmaWriteCommand with AutoExecuteRdmaRead=true:
//   1. Executes RDMA_READ to fetch data from source
//   2. Reconstructs original event
//   3. Sends to FinalTarget actor locally
//
// - TEvRdmaReadCommand with AutoExecuteRdmaWrite=true:
//   1. Executes RDMA_WRITE to send data to destination
//   2. Sends acknowledgment to source
////////////////////////////////////////////////////////////////////////////////

class TRdmaCqCommandHandler {
public:
    // Handle incoming TEvRdmaWriteCommand with auto-execution
    // This is called by CQ actor when RDMA RECV completes
    static void HandleWriteCommand(
        TActorSystem* actorSystem,
        TEvRdmaWriteCommand* cmd,
        std::shared_ptr<NInterconnect::NRdma::TQueuePair> qp,
        NInterconnect::NRdma::ICq::TPtr cq,
        NWilson::TSpan span);

    // Handle incoming TEvRdmaReadCommand with auto-execution
    static void HandleReadCommand(
        TActorSystem* actorSystem,
        TEvRdmaReadCommand* cmd,
        std::shared_ptr<NInterconnect::NRdma::TQueuePair> qp,
        NInterconnect::NRdma::ICq::TPtr cq,
        NWilson::TSpan span);
};

} // namespace NActors
