LIBRARY()

SRCS(
    ic_mock.cpp
    ic_mock.h
)

SUPPRESSIONS(tsan.supp)

PEERDIR(
    ydb/library/actors/interconnect
    ydb/library/actors/protos
    ydb/library/actors/core
)

END()
