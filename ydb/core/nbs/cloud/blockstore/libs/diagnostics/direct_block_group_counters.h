
#pragma once

#include "public.h"

#include "volume_counters.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/base.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TDirectBlockGroupCounters
{
private:
    // Counters for operations on persistent buffers
    TVolumeRequestCounters WritePersistentBuffer;
    TVolumeRequestCounters ErasePersistentBuffer;
    TVolumeRequestCounters SyncWithPersistentBuffer;
    TVolumeRequestCounters ReadPersistentBuffer;

    NMonitoring::TDynamicCounterPtr ParentCounters;

public:
    explicit TDirectBlockGroupCounters(
        NMonitoring::TDynamicCounterPtr rootCounters,
        const TString& ddiskPoolName,
        const TString& diskId,
        ui64 tabletId,
        ui64 directBlockGroupId);

    void WritePersistentBufferStarted(ui64 ddiskId, ui32 bytes);
    void WritePersistentBufferFinished(ui64 ddiskId, bool ok);

    void ErasePersistentBufferStarted(ui64 ddiskId, ui32 bytes);
    void ErasePersistentBufferFinished(ui64 ddiskId, bool ok);

    void SyncWithPersistentBufferStarted(ui64 ddiskId, ui32 bytes);
    void SyncWithPersistentBufferFinished(ui64 ddiskId, bool ok);

    void ReadPersistentBufferStarted(ui64 ddiskId, ui32 bytes);
    void ReadPersistentBufferFinished(ui64 ddiskId, bool ok);

private:
    NMonitoring::TDynamicCounterPtr GetPersistentBufferCounters(
        const TString& operation,
        ui64 ddiskId);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
