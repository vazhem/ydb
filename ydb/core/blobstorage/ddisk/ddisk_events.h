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

    struct TEvBlobStorage::TEvDDiskReserveChunksRequest : TEventPB<
        TEvBlobStorage::TEvDDiskReserveChunksRequest,
        NKikimrBlobStorage::TEvDDiskReserveChunksRequest,
        TEvBlobStorage::EvDDiskReserveChunksRequest>
    {
        TEvDDiskReserveChunksRequest() = default;

        TEvDDiskReserveChunksRequest(ui32 chunkCount) {
            Record.SetChunkCount(chunkCount);
        }

        TString ToString() const override {
            TStringStream str;
            str << "TEvDDiskReserveChunksRequest {ChunkCount# " << Record.GetChunkCount() << "}";
            return str.Str();
        }
    };

    struct TEvBlobStorage::TEvDDiskReserveChunksResponse : TEventPB<
        TEvBlobStorage::TEvDDiskReserveChunksResponse,
        NKikimrBlobStorage::TEvDDiskReserveChunksResponse,
        TEvBlobStorage::EvDDiskReserveChunksResponse>
    {
        TEvDDiskReserveChunksResponse() = default;

        TEvDDiskReserveChunksResponse(NKikimrProto::EReplyStatus status, const TString& reason = "") {
            Record.SetStatus(status);
            if (!reason.empty()) {
                Record.SetErrorReason(reason);
            }
        }

        TString ToString() const override {
            TStringStream str;
            str << "TEvDDiskReserveChunksResponse {Status# " << NKikimrProto::EReplyStatus_Name(Record.GetStatus()).data();
            if (!Record.GetErrorReason().empty()) {
                str << " ErrorReason# \"" << Record.GetErrorReason() << "\"";
            }
            str << " ChunkIds# [";
            for (int i = 0; i < static_cast<int>(Record.ChunkIdsSize()); ++i) {
                if (i > 0) str << ", ";
                str << Record.GetChunkIds(i);
            }
            str << "]}";
            return str.Str();
        }
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // DDisk Internal Events - Local events for DDisk worker coordination
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    // Event for updating chunk info in workers
    struct TEvDDiskChunkInfoUpdate : public TEventLocal<TEvDDiskChunkInfoUpdate, 1000> {
        struct TChunkUpdate {
            ui32 ChunkId;
            ui64 DeviceOffset;

            TChunkUpdate() = default;
            TChunkUpdate(ui32 chunkId, ui64 deviceOffset)
                : ChunkId(chunkId), DeviceOffset(deviceOffset) {}
        };

        TVector<TChunkUpdate> ChunkUpdates;

        TEvDDiskChunkInfoUpdate() = default;

        // Forward declaration for TDDiskActorImpl::TChunkInfo
        template<typename TChunkInfoMap>
        TEvDDiskChunkInfoUpdate(const TChunkInfoMap& chunkInfoMap) {
            for (const auto& [chunkId, chunkInfo] : chunkInfoMap) {
                if (chunkInfo.IsReserved) {
                    ChunkUpdates.emplace_back(chunkId, chunkInfo.DeviceOffset);
                }
            }
        }
    };

} // namespace NKikimr
