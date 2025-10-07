#pragma once

#include "ddisk_actor_impl.h"

namespace NKikimr {

//
// Direct I/O DDisk actor implementation
//
class TDDiskDirectIOActor final : public TDDiskActorImpl {
private:
    // DDiskWriter Direct Device I/O (bypasses PDisk)
    TActorId DDiskWriterId;

public:
    TDDiskDirectIOActor(
        TIntrusivePtr<TVDiskConfig> cfg,
        TIntrusivePtr<TBlobStorageGroupInfo> info,
        const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
        ui32 workerCount,
        ui32 chunksPerReservation);

protected:
    void ProcessReadRequest(
        const TEvBlobStorage::TEvDDiskReadRequest::TPtr& ev,
        const NActors::TActorContext& ctx) override;

    void ProcessWriteRequest(
        const TEvBlobStorage::TEvDDiskWriteRequest::TPtr& ev,
        const NActors::TActorContext& ctx) override;

private:
    // Common method for direct I/O operations
    template<typename TRequest, typename TResponse>
    void ProcessDirectIORequest(
        const TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NKikimr
