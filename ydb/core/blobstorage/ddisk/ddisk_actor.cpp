#include "ddisk_actor.h"
#include <ydb/core/blobstorage/ddisk/skeleton/ddisk_skeletonfront.h>

namespace NKikimr {

    // Forward declaration to ensure RDMA event registration
    void RegisterDDiskRdmaEvents();

    IActor* CreateDDisk(const TIntrusivePtr<TVDiskConfig> &cfg,
                        const TIntrusivePtr<TBlobStorageGroupInfo> &info,
                        const TIntrusivePtr<::NMonitoring::TDynamicCounters> &counters) {
        // Ensure DDisk RDMA events are registered before creating the actor
        RegisterDDiskRdmaEvents();

        // create front actor
        return CreateDDiskSkeletonFront(cfg, info, counters);
    }

} // NKikimr
