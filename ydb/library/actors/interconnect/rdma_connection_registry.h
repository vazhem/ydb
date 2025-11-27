#pragma once

#include <ydb/library/actors/interconnect/rdma/rdma.h>
#include <util/generic/hash.h>
#include <util/system/mutex.h>
#include <memory>

namespace NActors {

////////////////////////////////////////////////////////////////////////////////
// RDMA Connection Registry
//
// Thread-safe registry for RDMA connections indexed by node ID.
// Sessions register their RDMA connections when established and unregister
// when disconnecting.
////////////////////////////////////////////////////////////////////////////////

struct TRdmaConnectionInfo {
    std::shared_ptr<NInterconnect::NRdma::TQueuePair> Qp;
    NInterconnect::NRdma::ICq::TPtr Cq;
    ui32 PeerQpNum = 0;
    ui64 PeerGid[2] = {0, 0};
    ssize_t RdmaDeviceIndex = -1;

    TRdmaConnectionInfo() = default;

    TRdmaConnectionInfo(
        std::shared_ptr<NInterconnect::NRdma::TQueuePair> qp,
        NInterconnect::NRdma::ICq::TPtr cq,
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

    bool IsValid() const {
        return Qp != nullptr && Cq != nullptr;
    }
};

class TRdmaConnectionRegistry {
public:
    static TRdmaConnectionRegistry& Instance();

    // Register RDMA connection for a node
    void Register(ui32 nodeId, const TRdmaConnectionInfo& info);

    // Unregister RDMA connection for a node
    void Unregister(ui32 nodeId);

    // Get RDMA connection for a node
    std::optional<TRdmaConnectionInfo> Get(ui32 nodeId) const;

private:
    TRdmaConnectionRegistry() = default;

    mutable TMutex Mutex;
    THashMap<ui32, TRdmaConnectionInfo> Connections;
};

} // namespace NActors
