#pragma once

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/library/actors/core/event_pb.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // DDisk Events - Direct DDisk Operations (offset/size based)
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    struct TEvBlobStorage::TEvDDiskReadRequest
        : TEventPB<TEvBlobStorage::TEvDDiskReadRequest,
                   NKikimrBlobStorage::TEvDDiskReadRequest,
                   TEvBlobStorage::EvDDiskReadRequest> {

        TEvDDiskReadRequest() = default;

        TEvDDiskReadRequest(ui64 offset, ui32 size) {
            Record.SetOffset(offset);
            Record.SetSize(size);
        }

        TString ToString() const override {
            TStringStream str;
            str << "TEvDDiskReadRequest {Offset# " << Record.GetOffset()
                << " Size# " << Record.GetSize() << "}";
            return str.Str();
        }
    };

    struct TEvBlobStorage::TEvDDiskWriteRequest
        : TEventPB<TEvBlobStorage::TEvDDiskWriteRequest,
                   NKikimrBlobStorage::TEvDDiskWriteRequest,
                   TEvBlobStorage::EvDDiskWriteRequest> {

        TEvDDiskWriteRequest() = default;

        TEvDDiskWriteRequest(ui64 offset, ui32 size, const TString& data) {
            Record.SetOffset(offset);
            Record.SetSize(size);
            Record.SetData(data);
        }

        TString ToString() const override {
            TStringStream str;
            str << "TEvDDiskWriteRequest {Offset# " << Record.GetOffset()
                << " Size# " << Record.GetSize()
                << " DataSize# " << Record.GetData().size() << "}";
            return str.Str();
        }
    };

    struct TEvBlobStorage::TEvDDiskReadResponse
        : TEventPB<TEvBlobStorage::TEvDDiskReadResponse,
                   NKikimrBlobStorage::TEvDDiskReadResponse,
                   TEvBlobStorage::EvDDiskReadResponse> {

        TEvDDiskReadResponse() = default;

        TEvDDiskReadResponse(NKikimrProto::EReplyStatus status, const TString& reason = "") {
            Record.SetStatus(status);
            if (!reason.empty()) {
                Record.SetErrorReason(reason);
            }
        }

        TString ToString() const override {
            TStringStream str;
            str << "TEvDDiskReadResponse {Status# " << NKikimrProto::EReplyStatus_Name(Record.GetStatus()).data();
            if (!Record.GetErrorReason().empty()) {
                str << " ErrorReason# \"" << Record.GetErrorReason() << "\"";
            }
            str << " Offset# " << Record.GetOffset()
                << " Size# " << Record.GetSize()
                << " DataSize# " << Record.GetData().size() << "}";
            return str.Str();
        }
    };

    struct TEvBlobStorage::TEvDDiskWriteResponse
        : TEventPB<TEvBlobStorage::TEvDDiskWriteResponse,
                   NKikimrBlobStorage::TEvDDiskWriteResponse,
                   TEvBlobStorage::EvDDiskWriteResponse> {

        TEvDDiskWriteResponse() = default;

        TEvDDiskWriteResponse(NKikimrProto::EReplyStatus status, const TString& reason = "") {
            Record.SetStatus(status);
            if (!reason.empty()) {
                Record.SetErrorReason(reason);
            }
        }

        TString ToString() const override {
            TStringStream str;
            str << "TEvDDiskWriteResponse {Status# " << NKikimrProto::EReplyStatus_Name(Record.GetStatus()).data();
            if (!Record.GetErrorReason().empty()) {
                str << " ErrorReason# \"" << Record.GetErrorReason() << "\"";
            }
            str << " Offset# " << Record.GetOffset()
                << " Size# " << Record.GetSize() << "}";
            return str.Str();
        }
    };

} // namespace NKikimr
