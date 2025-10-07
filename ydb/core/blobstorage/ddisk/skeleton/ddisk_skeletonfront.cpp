#include "ddisk_skeletonfront.h"
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/ddisk/ddisk_actor_impl.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/blobstorage_ddisk_config.pb.h>

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

        NKikimrBlobStorage::TDDiskConfig ddiskConfig;
        if (cfg && cfg->BaseInfo.NodeWardenDDiskConfig) {
            ddiskConfig.CopyFrom(*cfg->BaseInfo.NodeWardenDDiskConfig);
        }

        // Determine the mode from config
        TDDiskActorImpl::EDDiskMode mode;
        switch (ddiskConfig.GetMode()) {
            case NKikimrBlobStorage::TDDiskConfig::MEMORY:
                mode = TDDiskActorImpl::EDDiskMode::MEMORY;
                break;
            case NKikimrBlobStorage::TDDiskConfig::PDISK_EVENTS:
                mode = TDDiskActorImpl::EDDiskMode::PDISK_EVENTS;
                break;
            case NKikimrBlobStorage::TDDiskConfig::DIRECT_IO:
                mode = TDDiskActorImpl::EDDiskMode::DIRECT_IO;
                break;
            default:
                // Default to DIRECT_IO if not specified or invalid
                mode = TDDiskActorImpl::EDDiskMode::DIRECT_IO;
                break;
        }

        // Get worker count and chunks per reservation from config
        ui32 workerCount = ddiskConfig.GetWorkerCount();
        ui32 chunksPerReservation = ddiskConfig.GetChunksPerReservation();

        // Log the selected mode and worker count
        LOG_INFO_S(TActivationContext::AsActorContext(), NKikimrServices::BS_DDISK,
            "Creating DDisk with mode: " << mode
            << ", WorkerCount: " << workerCount
            << ", ChunksPerReservation: " << chunksPerReservation);

        // Create the appropriate instance using the factory method
        return TDDiskActorImpl::Create(cfg, info, mode, counters, workerCount, chunksPerReservation).release();
    }

} // NKikimr
