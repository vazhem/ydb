#pragma once

#include "ddisk_actor_impl.h"

namespace NKikimr {

//
// PDisk events DDisk actor implementation
//
class TDDiskPDiskEventsActor final : public TDDiskActorImpl {
public:
    TDDiskPDiskEventsActor(
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
};

}   // namespace NKikimr
