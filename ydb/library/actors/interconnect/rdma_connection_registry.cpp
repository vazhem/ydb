#include "rdma_connection_registry.h"
#include <util/system/guard.h>

namespace NActors {

TRdmaConnectionRegistry& TRdmaConnectionRegistry::Instance() {
    static TRdmaConnectionRegistry instance;
    return instance;
}

void TRdmaConnectionRegistry::Register(ui32 nodeId, const TRdmaConnectionInfo& info) {
    TGuard<TMutex> guard(Mutex);
    Connections[nodeId] = info;
}

void TRdmaConnectionRegistry::Unregister(ui32 nodeId) {
    TGuard<TMutex> guard(Mutex);
    Connections.erase(nodeId);
}

std::optional<TRdmaConnectionInfo> TRdmaConnectionRegistry::Get(ui32 nodeId) const {
    TGuard<TMutex> guard(Mutex);
    auto it = Connections.find(nodeId);
    if (it == Connections.end()) {
        return std::nullopt;
    }
    return it->second;
}

} // namespace NActors

