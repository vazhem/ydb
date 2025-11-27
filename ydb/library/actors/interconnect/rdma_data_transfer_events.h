#pragma once

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/interconnect/rdma_data_transfer_proto/rdma_data_transfer.pb.h>
#include <ydb/library/actors/wilson/wilson_trace.h>

namespace NActors {

////////////////////////////////////////////////////////////////////////////////
// RDMA Message Header
//
// Similar to TEventDescr2 with TraceId support for distributed tracing.
// Format: [TRdmaMessageHeader][Serialized Event Data]
//
// Note: PayloadSize is not in the header - it's calculated from RDMA recv
// completion byte_len: payloadSize = byte_len - sizeof(TRdmaMessageHeader)
//
// RDMA provides hardware-level integrity so Checksum is not needed.
////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 1)
struct TRdmaMessageHeader {
    ui32 Type;          // Event type
    ui32 Flags;         // Event flags
    TActorId Recipient; // Target actor ID
    TActorId Sender;    // Source actor ID
    ui64 Cookie;        // Request cookie for matching request/response
    NWilson::TTraceId::TSerializedTraceId TraceId; // Trace ID for distributed tracing

    TRdmaMessageHeader() = default;

    TRdmaMessageHeader(ui32 type, ui32 flags, const TActorId& recipient,
                       const TActorId& sender, ui64 cookie, const NWilson::TTraceId& traceId = {})
        : Type(type)
        , Flags(flags)
        , Recipient(recipient)
        , Sender(sender)
        , Cookie(cookie)
    {
        if (traceId) {
            traceId.Serialize(&TraceId);
        } else {
            memset(TraceId, 0, sizeof(TraceId));
        }
    }
};
#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////
// RDMA Data Transfer Events
//
// Generic events for RDMA-based data transfer protocol using protobuf:
// 1. Source sends command with memory region info
// 2. Target performs RDMA operation (READ or WRITE)
// 3. Target sends acknowledgment back to source
////////////////////////////////////////////////////////////////////////////////

enum ERdmaDataTransferEvents {
    EvBegin = EventSpaceBegin(TEvents::ES_INTERCONNECT_RDMA),

    // Write protocol: SRC provides data location, TGT reads via RDMA
    EvRdmaWriteCommand = EvBegin + 10,
    EvRdmaWriteAck,

    // Read protocol: SRC provides buffer location, TGT writes via RDMA
    EvRdmaReadCommand,
    EvRdmaReadAck,

    EvEnd
};

////////////////////////////////////////////////////////////////////////////////
// RDMA Memory Region Helper
////////////////////////////////////////////////////////////////////////////////

struct TRdmaMemoryRegion {
    ui64 Address = 0;  // Remote memory address
    ui32 Lkey = 0;     // Local key for RDMA access
    ui32 Rkey = 0;     // Remote key for RDMA access
    ui32 Size = 0;     // Size of the region

    TRdmaMemoryRegion() = default;

    TRdmaMemoryRegion(ui64 address, ui32 lkey, ui32 rkey, ui32 size)
        : Address(address)
        , Lkey(lkey)
        , Rkey(rkey)
        , Size(size)
    {}

    // Constructor for backward compatibility (without Lkey)
    TRdmaMemoryRegion(ui64 address, ui32 rkey, ui32 size)
        : Address(address)
        , Lkey(0)
        , Rkey(rkey)
        , Size(size)
    {}

    bool IsValid() const {
        return Address != 0 && Rkey != 0 && Size != 0;
    }

    // Convert to protobuf message
    void ToProto(NActorsInterconnect::TRdmaMemoryRegion& proto) const {
        proto.SetAddress(Address);
        proto.SetLkey(Lkey);
        proto.SetRkey(Rkey);
        proto.SetSize(Size);
    }

    // Create from protobuf message
    static TRdmaMemoryRegion FromProto(const NActorsInterconnect::TRdmaMemoryRegion& proto) {
        return TRdmaMemoryRegion(
            proto.GetAddress(),
            proto.GetLkey(),
            proto.GetRkey(),
            proto.GetSize()
        );
    }
};

////////////////////////////////////////////////////////////////////////////////
// TEvRdmaWriteCommand - Write request with source data location (Protobuf-based)
////////////////////////////////////////////////////////////////////////////////

struct TEvRdmaWriteCommand : public TEventPB<TEvRdmaWriteCommand,
                                              NActorsInterconnect::TEvRdmaWriteCommand,
                                              EvRdmaWriteCommand> {
    TEvRdmaWriteCommand() = default;

    TEvRdmaWriteCommand(
        ui64 cookie,
        TActorId responseTarget,
        const TRdmaMemoryRegion& sourceRegion,
        TString applicationPayload = {},
        ui32 embeddedEventType = 0,
        TActorId finalTarget = {},
        bool autoExecuteRdmaRead = false)
    {
        Record.SetCookie(cookie);
        ActorIdToProto(responseTarget, Record.MutableResponseTarget());
        ActorIdToProto(finalTarget, Record.MutableFinalTarget());
        sourceRegion.ToProto(*Record.MutableSourceRegion());
        Record.SetApplicationPayload(std::move(applicationPayload));
        Record.SetEmbeddedEventType(embeddedEventType);
        Record.SetAutoExecuteRdmaRead(autoExecuteRdmaRead);
    }

    // Helper accessors
    ui64 GetCookie() const { return Record.GetCookie(); }
    TActorId GetResponseTarget() const { return ActorIdFromProto(Record.GetResponseTarget()); }
    TActorId GetFinalTarget() const { return ActorIdFromProto(Record.GetFinalTarget()); }
    TRdmaMemoryRegion GetSourceRegion() const { return TRdmaMemoryRegion::FromProto(Record.GetSourceRegion()); }
    const TString& GetApplicationPayload() const { return Record.GetApplicationPayload(); }
    ui32 GetEmbeddedEventType() const { return Record.GetEmbeddedEventType(); }
    bool GetAutoExecuteRdmaRead() const { return Record.GetAutoExecuteRdmaRead(); }
};

////////////////////////////////////////////////////////////////////////////////
// TEvRdmaDataReady - Notification that RDMA transfer completed (auto-execution)
// This is a local event, not serialized
////////////////////////////////////////////////////////////////////////////////

struct TEvRdmaDataReady : public TEventLocal<TEvRdmaDataReady, EvRdmaWriteCommand + 100> {
    ui64 Cookie;
    TActorId ResponseTarget;
    TRdmaMemoryRegion DataRegion;  // Local buffer with data
    TString ApplicationPayload;
    bool IsSuccess;
    TString ErrorMessage;

    TEvRdmaDataReady(
        ui64 cookie,
        TActorId responseTarget,
        const TRdmaMemoryRegion& dataRegion,
        TString applicationPayload,
        bool isSuccess = true,
        TString errorMessage = {})
        : Cookie(cookie)
        , ResponseTarget(responseTarget)
        , DataRegion(dataRegion)
        , ApplicationPayload(std::move(applicationPayload))
        , IsSuccess(isSuccess)
        , ErrorMessage(std::move(errorMessage))
    {}
};

////////////////////////////////////////////////////////////////////////////////
// TEvRdmaWriteAck - Acknowledgment for write operation (Protobuf-based)
////////////////////////////////////////////////////////////////////////////////

struct TEvRdmaWriteAck : public TEventPB<TEvRdmaWriteAck,
                                          NActorsInterconnect::TEvRdmaWriteAck,
                                          EvRdmaWriteAck> {
    TEvRdmaWriteAck() = default;

    TEvRdmaWriteAck(
        ui64 cookie,
        i32 errorCode = 0,
        TString errorMessage = {},
        TString applicationResponse = {})
    {
        Record.SetCookie(cookie);
        Record.SetErrorCode(errorCode);
        if (!errorMessage.empty()) {
            Record.SetErrorMessage(std::move(errorMessage));
        }
        if (!applicationResponse.empty()) {
            Record.SetApplicationResponse(std::move(applicationResponse));
        }
    }

    // Helper accessors
    ui64 GetCookie() const { return Record.GetCookie(); }
    i32 GetErrorCode() const { return Record.GetErrorCode(); }
    const TString& GetErrorMessage() const { return Record.GetErrorMessage(); }
    const TString& GetApplicationResponse() const { return Record.GetApplicationResponse(); }
};

////////////////////////////////////////////////////////////////////////////////
// TEvRdmaReadCommand - Read request with destination buffer location (Protobuf-based)
////////////////////////////////////////////////////////////////////////////////

struct TEvRdmaReadCommand : public TEventPB<TEvRdmaReadCommand,
                                             NActorsInterconnect::TEvRdmaReadCommand,
                                             EvRdmaReadCommand> {
    TEvRdmaReadCommand() = default;

    TEvRdmaReadCommand(
        ui64 cookie,
        TActorId responseTarget,
        const TRdmaMemoryRegion& destinationRegion,
        TString applicationPayload = {},
        ui32 embeddedEventType = 0,
        TActorId finalTarget = {},
        bool autoExecuteRdmaWrite = false)
    {
        Record.SetCookie(cookie);
        ActorIdToProto(responseTarget, Record.MutableResponseTarget());
        ActorIdToProto(finalTarget, Record.MutableFinalTarget());
        destinationRegion.ToProto(*Record.MutableDestinationRegion());
        Record.SetApplicationPayload(std::move(applicationPayload));
        Record.SetEmbeddedEventType(embeddedEventType);
        Record.SetAutoExecuteRdmaWrite(autoExecuteRdmaWrite);
    }

    // Helper accessors
    ui64 GetCookie() const { return Record.GetCookie(); }
    TActorId GetResponseTarget() const { return ActorIdFromProto(Record.GetResponseTarget()); }
    TActorId GetFinalTarget() const { return ActorIdFromProto(Record.GetFinalTarget()); }
    TRdmaMemoryRegion GetDestinationRegion() const { return TRdmaMemoryRegion::FromProto(Record.GetDestinationRegion()); }
    const TString& GetApplicationPayload() const { return Record.GetApplicationPayload(); }
    ui32 GetEmbeddedEventType() const { return Record.GetEmbeddedEventType(); }
    bool GetAutoExecuteRdmaWrite() const { return Record.GetAutoExecuteRdmaWrite(); }
};

////////////////////////////////////////////////////////////////////////////////
// TEvRdmaReadAck - Acknowledgment for read operation (Protobuf-based)
////////////////////////////////////////////////////////////////////////////////

struct TEvRdmaReadAck : public TEventPB<TEvRdmaReadAck,
                                         NActorsInterconnect::TEvRdmaReadAck,
                                         EvRdmaReadAck> {
    TEvRdmaReadAck() = default;

    TEvRdmaReadAck(
        ui64 cookie,
        i32 errorCode = 0,
        TString errorMessage = {},
        TString applicationResponse = {})
    {
        Record.SetCookie(cookie);
        Record.SetErrorCode(errorCode);
        if (!errorMessage.empty()) {
            Record.SetErrorMessage(std::move(errorMessage));
        }
        if (!applicationResponse.empty()) {
            Record.SetApplicationResponse(std::move(applicationResponse));
        }
    }

    // Helper accessors
    ui64 GetCookie() const { return Record.GetCookie(); }
    i32 GetErrorCode() const { return Record.GetErrorCode(); }
    const TString& GetErrorMessage() const { return Record.GetErrorMessage(); }
    const TString& GetApplicationResponse() const { return Record.GetApplicationResponse(); }
};

} // namespace NActors
