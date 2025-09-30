#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

namespace NKikimr {

    IActor* CreateDDisk(const TIntrusivePtr<TVDiskConfig> &cfg,
                        const TIntrusivePtr<TBlobStorageGroupInfo> &info,
                        const TIntrusivePtr<::NMonitoring::TDynamicCounters> &counters);

} // NKikimr
