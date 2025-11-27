LIBRARY()

NO_WSHADOW()

IF (PROFILE_MEMORY_ALLOCATIONS)
    CFLAGS(-DPROFILE_MEMORY_ALLOCATIONS)
ENDIF()

IF (MUSL)
    # musl code for CMSG_NXTHDR is broken by this check
    CFLAGS(-Wno-sign-compare)
ENDIF()

SRCS(
    channel_scheduler.h
    cq_actor.cpp
    event_filter.h
    event_holder_pool.h
    events_local.h
    interconnect_address.cpp
    interconnect_address.h
    interconnect_channel.cpp
    interconnect_channel.h
    interconnect_common.h
    interconnect_counters.cpp
    interconnect.h
    interconnect_handshake.cpp
    interconnect_handshake.h
    interconnect_impl.h
    interconnect_mon.cpp
    interconnect_mon.h
    interconnect_nameserver_dynamic.cpp
    interconnect_nameserver_table.cpp
    interconnect_proxy_wrapper.cpp
    interconnect_proxy_wrapper.h
    interconnect_rdma_api.cpp
    interconnect_rdma_api.h
    interconnect_rdma_transport.cpp
    interconnect_rdma_transport.h
    interconnect_resolve.cpp
    interconnect_stream.cpp
    interconnect_stream.h
    interconnect_tcp_input_session.cpp
    interconnect_tcp_proxy.cpp
    interconnect_tcp_proxy.h
    interconnect_tcp_server.cpp
    interconnect_tcp_server.h
    interconnect_tcp_session.cpp
    interconnect_tcp_session.h
    interconnect_zc_processor.cpp
    interconnect_zc_processor.h
    load.cpp
    load.h
    logging.h
    packet.cpp
    packet.h
    poller_actor.cpp
    poller_actor.h
    poller.h
    poller_tcp.cpp
    poller_tcp.h
    poller_tcp_unit.cpp
    poller_tcp_unit.h
    poller_tcp_unit_select.cpp
    poller_tcp_unit_select.h
    profiler.h
    rdma_cq_command_handler.cpp
    rdma_cq_command_handler.h
    rdma_recv_handler.cpp
    rdma_recv_handler.h
    rdma_connection_registry.cpp
    rdma_connection_registry.h
    rdma_qp_registry.cpp
    rdma_qp_registry.h
    rdma_data_transfer_events.h
    rdma_event_base.h
    rdma_event_serializer.cpp
    rdma_event_serializer.h
    rdma_memory_region.cpp
    rdma_memory_region.h
    slowpoke_actor.h
    subscription_manager.cpp
    subscription_manager.h
    types.cpp
    types.h
    watchdog_timer.h
)

IF (OS_LINUX)
    SRCS(
        poller_tcp_unit_epoll.cpp
        poller_tcp_unit_epoll.h
    )
ENDIF()

PEERDIR(
    contrib/libs/libc_compat
    contrib/libs/openssl
    contrib/libs/xxhash
    ydb/library/actors/core
    ydb/library/actors/dnscachelib
    ydb/library/actors/dnsresolver
    ydb/library/actors/helpers
    ydb/library/actors/interconnect/rdma
    ydb/library/actors/interconnect/rdma_data_transfer_proto
    ydb/library/actors/prof
    ydb/library/actors/protos
    ydb/library/actors/util
    ydb/library/actors/wilson
    library/cpp/digest/crc32c
    library/cpp/json
    library/cpp/lwtrace
    library/cpp/monlib/dynamic_counters
    library/cpp/monlib/metrics
    library/cpp/monlib/service/pages/resources
    library/cpp/monlib/service/pages/tablesorter
    library/cpp/openssl/init
    library/cpp/packedtypes
)

END()

RECURSE(
    rdma
)

RECURSE_FOR_TESTS(
    ut
    ut_fat
    ut_huge_cluster
)
