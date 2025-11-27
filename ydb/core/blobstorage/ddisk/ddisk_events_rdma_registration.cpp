#include "ddisk_events.h"
#include <ydb/library/actors/interconnect/rdma_event_serializer.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/protos/services_common.pb.h>

namespace NKikimr {

// Explicit initialization function to register DDisk events with RDMA system
// This must be called during program initialization
void RegisterDDiskRdmaEvents() {
    static bool registered = false;
    if (registered) {
        return;
    }
    registered = true;

    // Register factories for DDisk events
    NActors::RegisterEventFactory(
        TEvBlobStorage::EvDDiskWriteRequest,
        []() -> std::unique_ptr<NActors::IEventBase> {
            return std::make_unique<TEvBlobStorage::TEvDDiskWriteRequest>();
        });

    NActors::RegisterEventFactory(
        TEvBlobStorage::EvDDiskReadRequest,
        []() -> std::unique_ptr<NActors::IEventBase> {
            return std::make_unique<TEvBlobStorage::TEvDDiskReadRequest>();
        });

    NActors::RegisterEventFactory(
        TEvBlobStorage::EvDDiskReadResponse,
        []() -> std::unique_ptr<NActors::IEventBase> {
            return std::make_unique<TEvBlobStorage::TEvDDiskReadResponse>();
        });

    NActors::RegisterEventFactory(
        TEvBlobStorage::EvDDiskWriteResponse,
        []() -> std::unique_ptr<NActors::IEventBase> {
            return std::make_unique<TEvBlobStorage::TEvDDiskWriteResponse>();
        });

    // Log that registration is complete
    Cerr << "DDisk RDMA event factories registered:" << Endl;
    Cerr << "  EvDDiskWriteRequest = " << static_cast<ui32>(TEvBlobStorage::EvDDiskWriteRequest) << Endl;
    Cerr << "  EvDDiskReadRequest = " << static_cast<ui32>(TEvBlobStorage::EvDDiskReadRequest) << Endl;
    Cerr << "  EvDDiskReadResponse = " << static_cast<ui32>(TEvBlobStorage::EvDDiskReadResponse) << Endl;
    Cerr << "  EvDDiskWriteResponse = " << static_cast<ui32>(TEvBlobStorage::EvDDiskWriteResponse) << Endl;
}

// Static initializer to ensure registration happens at startup
// This may not always work due to linker optimizations, so we also provide
// an explicit RegisterDDiskRdmaEvents() function
struct TDDiskEventsRdmaRegistrar {
    TDDiskEventsRdmaRegistrar() {
        RegisterDDiskRdmaEvents();
    }
};

static TDDiskEventsRdmaRegistrar gDDiskEventsRdmaRegistrar;

} // namespace NKikimr
