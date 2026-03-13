
#include "direct_block_group_counters.h"

#include <ydb/core/base/counters.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TDirectBlockGroupCounters::TDirectBlockGroupCounters(
    NMonitoring::TDynamicCounterPtr rootCounters,
    const TString& ddiskPoolName,
    const TString& diskId,
    ui64 tabletId,
    ui64 directBlockGroupId)
    : WritePersistentBuffer(nullptr)
    , ErasePersistentBuffer(nullptr)
    , SyncWithPersistentBuffer(nullptr)
    , ReadPersistentBuffer(nullptr)
    , ParentCounters(nullptr)
{
    if (rootCounters) {
        auto counters =
            NKikimr::GetServiceCounters(rootCounters, "nbs_partitions");
        counters = counters->GetSubgroup("ddiskPool", ddiskPoolName);
        counters = counters->GetSubgroup("diskId", diskId);
        counters = counters->GetSubgroup("subsystem", "directBlockGroup");
        counters = counters->GetSubgroup("tabletId", ToString(tabletId));
        counters = counters->GetSubgroup(
            "directBlockGroupId",
            ToString(directBlockGroupId));
        ParentCounters = counters;
    }
}

NMonitoring::TDynamicCounterPtr
TDirectBlockGroupCounters::GetPersistentBufferCounters(
    const TString& operation,
    ui64 ddiskId)
{
    if (!ParentCounters) {
        return nullptr;
    }

    auto counters = ParentCounters->GetSubgroup("operation", operation);
    counters = counters->GetSubgroup("ddiskId", ToString(ddiskId));
    return counters;
}

void TDirectBlockGroupCounters::WritePersistentBufferStarted(
    ui64 ddiskId,
    ui32 bytes)
{
    auto counters =
        GetPersistentBufferCounters("WritePersistentBuffer", ddiskId);
    TVolumeRequestCounters(counters).RequestStarted(bytes);
}

void TDirectBlockGroupCounters::WritePersistentBufferFinished(
    ui64 ddiskId,
    bool ok)
{
    auto counters =
        GetPersistentBufferCounters("WritePersistentBuffer", ddiskId);
    TVolumeRequestCounters(counters).RequestFinished(ok);
}

void TDirectBlockGroupCounters::ErasePersistentBufferStarted(
    ui64 ddiskId,
    ui32 bytes)
{
    auto counters =
        GetPersistentBufferCounters("ErasePersistentBuffer", ddiskId);
    TVolumeRequestCounters(counters).RequestStarted(bytes);
}

void TDirectBlockGroupCounters::ErasePersistentBufferFinished(
    ui64 ddiskId,
    bool ok)
{
    auto counters =
        GetPersistentBufferCounters("ErasePersistentBuffer", ddiskId);
    TVolumeRequestCounters(counters).RequestFinished(ok);
}

void TDirectBlockGroupCounters::SyncWithPersistentBufferStarted(
    ui64 ddiskId,
    ui32 bytes)
{
    auto counters =
        GetPersistentBufferCounters("SyncWithPersistentBuffer", ddiskId);
    TVolumeRequestCounters(counters).RequestStarted(bytes);
}

void TDirectBlockGroupCounters::SyncWithPersistentBufferFinished(
    ui64 ddiskId,
    bool ok)
{
    auto counters =
        GetPersistentBufferCounters("SyncWithPersistentBuffer", ddiskId);
    TVolumeRequestCounters(counters).RequestFinished(ok);
}

void TDirectBlockGroupCounters::ReadPersistentBufferStarted(
    ui64 ddiskId,
    ui32 bytes)
{
    auto counters =
        GetPersistentBufferCounters("ReadPersistentBuffer", ddiskId);
    TVolumeRequestCounters(counters).RequestStarted(bytes);
}

void TDirectBlockGroupCounters::ReadPersistentBufferFinished(
    ui64 ddiskId,
    bool ok)
{
    auto counters =
        GetPersistentBufferCounters("ReadPersistentBuffer", ddiskId);
    TVolumeRequestCounters(counters).RequestFinished(ok);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
