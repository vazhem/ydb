#include "ddisk_actor.h"
#include <ydb/core/blobstorage/ddisk/skeleton/ddisk_skeletonfront.h>

namespace NKikimr {

    IActor* CreateDDisk(const TIntrusivePtr<TVDiskConfig> &cfg,
                        const TIntrusivePtr<TBlobStorageGroupInfo> &info,
                        const TIntrusivePtr<::NMonitoring::TDynamicCounters> &counters) {
        // create front actor
        return CreateDDiskSkeletonFront(cfg, info, counters);
    }

} // NKikimr
