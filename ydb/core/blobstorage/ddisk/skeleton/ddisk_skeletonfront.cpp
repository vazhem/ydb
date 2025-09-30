#include "ddisk_skeletonfront.h"
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/ddisk/ddisk_actor_impl.h>
#include <util/system/env.h>

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
        // DDisk will initialize PDisk connection during Bootstrap via TEvYardInit
        // No need to create mock PDisk context - it will be established dynamically

        // Determine the mode based on environment variable
        TDDiskActorImpl::EDDiskMode mode;
        TString ddiskMode = GetEnv("DDISK_MODE");

        if (ddiskMode == "MEMORY") {
            mode = TDDiskActorImpl::EDDiskMode::MEMORY;
        } else if (ddiskMode == "PDISK_EVENTS") {
            mode = TDDiskActorImpl::EDDiskMode::PDISK_EVENTS;
        } else if (ddiskMode == "DIRECT_IO") {
            mode = TDDiskActorImpl::EDDiskMode::DIRECT_IO;
        } else {
            // Default to DIRECT_IO if not specified or invalid
            mode = TDDiskActorImpl::EDDiskMode::DIRECT_IO;
        }

        // Log the selected mode
        LOG_INFO_S(TActivationContext::AsActorContext(), NKikimrServices::BS_DDISK,
            "Using DDisk mode: " << mode);
        // Create the appropriate instance using the factory method
        return TDDiskActorImpl::Create(cfg, info, mode, counters).release();
    }

} // NKikimr
