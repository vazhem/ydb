#include "ddisk_skeletonfront.h"
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

using namespace NKikimrServices;

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // DDisk Skeleton Front - empty stub for ErasureMirror3Direct
    ////////////////////////////////////////////////////////////////////////////

    class TDDiskSkeletonFront : public TActorBootstrapped<TDDiskSkeletonFront> {
        friend class TActorBootstrapped<TDDiskSkeletonFront>;

        TIntrusivePtr<TVDiskConfig> Config;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        TVDiskID SelfVDiskId;

        void Bootstrap(const TActorContext &ctx) {
            Y_UNUSED(ctx);
            SelfVDiskId = GInfo->GetVDiskId(Config->BaseInfo.VDiskIdShort);
            Become(&TThis::StateFunc);
        }

        STRICT_STFUNC(StateFunc,
            CFunc(NActors::TEvents::TSystem::PoisonPill, Die)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_SKELETON_FRONT;
        }

        TDDiskSkeletonFront(TIntrusivePtr<TVDiskConfig> cfg, TIntrusivePtr<TBlobStorageGroupInfo> info,
                           const TIntrusivePtr<::NMonitoring::TDynamicCounters>&)
            : TActorBootstrapped<TDDiskSkeletonFront>()
            , Config(cfg)
            , GInfo(info)
            , SelfVDiskId()
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // DDisk SKELETON FRONT CREATOR
    ////////////////////////////////////////////////////////////////////////////
    IActor* CreateDDiskSkeletonFront(const TIntrusivePtr<TVDiskConfig> &cfg,
                                     const TIntrusivePtr<TBlobStorageGroupInfo> &info,
                                     const TIntrusivePtr<::NMonitoring::TDynamicCounters> &counters) {
        return new TDDiskSkeletonFront(cfg, info, counters);
    }

} // NKikimr
