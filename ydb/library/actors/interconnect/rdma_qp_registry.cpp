#include "rdma_qp_registry.h"
#include <util/system/guard.h>

namespace NActors {

TRdmaQpRegistry& TRdmaQpRegistry::Instance() {
    static TRdmaQpRegistry instance;
    return instance;
}

void TRdmaQpRegistry::Register(ui32 qpNum, const TRdmaQpInfo& info) {
    TGuard<TMutex> guard(Mutex);
    QpMap[qpNum] = info;
}

void TRdmaQpRegistry::Unregister(ui32 qpNum) {
    TGuard<TMutex> guard(Mutex);
    QpMap.erase(qpNum);
}

std::optional<TRdmaQpInfo> TRdmaQpRegistry::Get(ui32 qpNum) const {
    TGuard<TMutex> guard(Mutex);
    auto it = QpMap.find(qpNum);
    if (it == QpMap.end()) {
        return std::nullopt;
    }
    return it->second;
}

} // namespace NActors
