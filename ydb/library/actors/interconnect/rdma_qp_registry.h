#pragma once

#include <ydb/library/actors/interconnect/rdma/rdma.h>
#include <util/generic/hash.h>
#include <util/system/mutex.h>
#include <memory>

namespace NActors {

////////////////////////////////////////////////////////////////////////////////
// RDMA QP Number Registry
//
// Thread-safe registry for RDMA connections indexed by QP number.
// This is used by the CQ polling loop to look up connection info when
// processing RECV completions.
////////////////////////////////////////////////////////////////////////////////

struct TRdmaQpInfo {
    std::shared_ptr<NInterconnect::NRdma::TQueuePair> Qp;
    NInterconnect::NRdma::ICq::TPtr Cq;
    ssize_t RdmaDeviceIndex = -1;

    TRdmaQpInfo() = default;

    TRdmaQpInfo(
        std::shared_ptr<NInterconnect::NRdma::TQueuePair> qp,
        NInterconnect::NRdma::ICq::TPtr cq,
        ssize_t rdmaDeviceIndex)
        : Qp(std::move(qp))
        , Cq(std::move(cq))
        , RdmaDeviceIndex(rdmaDeviceIndex)
    {
    }

    bool IsValid() const {
        return Qp != nullptr && Cq != nullptr;
    }
};

class TRdmaQpRegistry {
public:
    static TRdmaQpRegistry& Instance();

    // Register QP by its QP number
    void Register(ui32 qpNum, const TRdmaQpInfo& info);

    // Unregister QP by its QP number
    void Unregister(ui32 qpNum);

    // Get QP info by QP number
    std::optional<TRdmaQpInfo> Get(ui32 qpNum) const;

private:
    TRdmaQpRegistry() = default;

    mutable TMutex Mutex;
    THashMap<ui32, TRdmaQpInfo> QpMap;
};

} // namespace NActors
