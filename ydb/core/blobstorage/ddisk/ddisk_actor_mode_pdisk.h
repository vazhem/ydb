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

private:
    // Override base class handlers to avoid forwarding to workers for PDISK_EVENTS mode
    void HandleReadRequest(
        const TEvBlobStorage::TEvDDiskReadRequest::TPtr& ev,
        const NActors::TActorContext& ctx) override;

    void HandleWriteRequest(
        const TEvBlobStorage::TEvDDiskWriteRequest::TPtr& ev,
        const NActors::TActorContext& ctx) override;

    void HandleChunkReadResult(
        const NPDisk::TEvChunkReadResult::TPtr& ev,
        const NActors::TActorContext& ctx) override;

    void HandleChunkWriteResult(
        const NPDisk::TEvChunkWriteResult::TPtr& ev,
        const NActors::TActorContext& ctx) override;
};

}   // namespace NKikimr
