#pragma once

#include "ddisk_actor_impl.h"

namespace NKikimr {

//
// In-memory DDisk actor implementation
//
class TDDiskMemoryActor final : public TDDiskActorImpl {
private:
    // In-memory chunk data structure
    struct TChunkData {
        TString Data;

        TChunkData() = default;
        TChunkData(const TString& data) : Data(data) {}

        TString ReadData(ui32 offset, ui32 size) const {
            if (offset >= Data.size()) {
                return TString(size, '\0');
            }
            ui32 availableSize = Min(size, static_cast<ui32>(Data.size()) - offset);
            return Data.substr(offset, availableSize) + TString(size - availableSize, '\0');
        }

        void WriteData(ui32 offset, const TString& data) {
            if (offset + data.size() > Data.size()) {
                Data.resize(offset + data.size());
            }
            memcpy(const_cast<char*>(Data.data()) + offset, data.data(), data.size());
        }
    };

    THashMap<ui32, TChunkData> InMemoryChunks;

public:
    TDDiskMemoryActor(
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
    // Override base class handlers to avoid forwarding to workers for memory mode
    void HandleReadRequest(
        const TEvBlobStorage::TEvDDiskReadRequest::TPtr& ev,
        const NActors::TActorContext& ctx) override;

    void HandleWriteRequest(
        const TEvBlobStorage::TEvDDiskWriteRequest::TPtr& ev,
        const NActors::TActorContext& ctx) override;
};

}   // namespace NKikimr
