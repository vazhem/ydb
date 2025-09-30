#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>
#include <ydb/core/blobstorage/ddisk/ddisk_events.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

#include <util/generic/hash.h>

namespace NKikimr {

////////////////////////////////////////////////////////////////////////////////

//
// DDisk events for direct operations (offset/size based)
//
// DDisk events are now defined in blobstorage.h as part of TEvBlobStorage

////////////////////////////////////////////////////////////////////////////////

//
// DDisk actor implementation for ErasureMirror3Direct
//
class TDDiskActorImpl final
    : public NActors::TActorBootstrapped<TDDiskActorImpl>
{
private:
    TIntrusivePtr<TVDiskConfig> Config;
    TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
    TVDiskID SelfVDiskId;

    // Simple in-memory storage for testing
    THashMap<ui64, TString> Data;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_SKELETON_FRONT;
    }

    TDDiskActorImpl(
        TIntrusivePtr<TVDiskConfig> cfg,
        TIntrusivePtr<TBlobStorageGroupInfo> info,
        const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
        : Config(std::move(cfg))
        , GInfo(std::move(info))
    {
        Y_UNUSED(counters);
    }

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvDDiskReadRequest, HandleReadRequest);
            HFunc(TEvBlobStorage::TEvDDiskWriteRequest, HandleWriteRequest);

            CFunc(NActors::TEvents::TSystem::PoisonPill, Die);

            default:
                Y_ABORT("Unexpected event type: %" PRIx32 " event: %s",
                       ev->GetTypeRewrite(), ev->ToString().data());
        }
    }

    void HandleReadRequest(
        const TEvBlobStorage::TEvDDiskReadRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteRequest(
        const TEvBlobStorage::TEvDDiskWriteRequest::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NKikimr
