PROTO_LIBRARY()

SRCS(
    rdma_data_transfer.proto
)

EXCLUDE_TAGS(GO_PROTO)

PEERDIR(
    ydb/library/actors/protos
)

END()
