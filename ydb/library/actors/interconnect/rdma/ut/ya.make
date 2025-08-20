GTEST()


IF (OS_LINUX)

CFLAGS(
    -DIC_RDMA_GTEST
)

SRCS(
    allocator_ut.cpp
    ibv_ut.cpp
    rdma_low_ut.cpp
)

PEERDIR(
    ydb/library/actors/interconnect/rdma/ibdrv
    ydb/library/actors/interconnect/rdma
    ydb/library/actors/interconnect
    ydb/library/actors/testlib
)

ENDIF()

END()
