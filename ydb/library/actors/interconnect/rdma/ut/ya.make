UNITTEST()

IF (OS_LINUX)

SRCS(
    allocator_ut.cpp
    ibv_ut.cpp
    link_manager_ut.cpp
)

PEERDIR(
    ydb/library/actors/interconnect/rdma/ibdrv
    ydb/library/actors/interconnect/rdma
)

ENDIF()

END()
