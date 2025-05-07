UNITTEST()

IF (OS_LINUX)

SRCS(
    ibv_ut.cpp
    rdma_link_manager_ut.cpp
)

PEERDIR(
    ydb/library/actors/interconnect/rdma/ibdrv
    ydb/library/actors/interconnect/rdma
)

ENDIF()

END()
