#include "ddisk_actor_impl.h"

#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TDDiskActorImpl::Bootstrap(const NActors::TActorContext& ctx)
{
    SelfVDiskId = GInfo->GetVDiskId(Config->BaseInfo.VDiskIdShort);

    LOG_INFO_S(ctx, NKikimrServices::BS_SKELETON,
        "DDiskActorImpl: Bootstrap complete for VDiskId=" << SelfVDiskId.ToString());

    Become(&TThis::StateWork);
}

void TDDiskActorImpl::HandleReadRequest(
    const TEvBlobStorage::TEvDDiskReadRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const ui64 offset = msg->Record.GetOffset();
    const ui32 size = msg->Record.GetSize();

    LOG_DEBUG_S(ctx, NKikimrServices::BS_SKELETON,
        "DDiskActorImpl: HandleReadRequest offset=" << offset
        << " size=" << size << " from=" << ev->Sender.ToString());

    // Find data in our simple storage
    auto it = Data.find(offset);
    TString data;
    NKikimrProto::EReplyStatus status = NKikimrProto::OK;
    TString errorReason;

    if (it != Data.end() && it->second.size() >= size) {
        // Return existing data
        data = it->second.substr(0, size);
    } else {
        // Return zeros for unwritten data
        data = TString(size, 0);
    }

    // Create response
    auto response = std::make_unique<TEvBlobStorage::TEvDDiskReadResponse>();
    response->Record.SetStatus(status);
    response->Record.SetErrorReason(errorReason);
    response->Record.SetOffset(offset);
    response->Record.SetSize(size);
    response->Record.SetData(std::move(data));

    // Send response back
    ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
}

void TDDiskActorImpl::HandleWriteRequest(
    const TEvBlobStorage::TEvDDiskWriteRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const ui64 offset = msg->Record.GetOffset();
    const ui32 size = msg->Record.GetSize();
    const TString& data = msg->Record.GetData();

    LOG_DEBUG_S(ctx, NKikimrServices::BS_SKELETON,
        "DDiskActorImpl: HandleWriteRequest offset=" << offset
        << " size=" << size << " from=" << ev->Sender.ToString());

    // Validate data size
    NKikimrProto::EReplyStatus status = NKikimrProto::OK;
    TString errorReason;

    if (data.size() != size) {
        status = NKikimrProto::ERROR;
        errorReason = TStringBuilder() << "Data size mismatch: expected=" << size
                                       << " actual=" << data.size();
    } else {
        // Store data in our simple storage
        Data[offset] = data;
    }

    // Create response
    auto response = std::make_unique<TEvBlobStorage::TEvDDiskWriteResponse>();
    response->Record.SetStatus(status);
    response->Record.SetErrorReason(errorReason);
    response->Record.SetOffset(offset);
    response->Record.SetSize(size);

    // Send response back
    ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
}

}   // namespace NKikimr
