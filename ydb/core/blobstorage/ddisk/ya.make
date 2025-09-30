LIBRARY()

SRCS(
    ddisk_actor.cpp
    skeleton/ddisk_skeletonfront.cpp
)

PEERDIR(
    ydb/core/blobstorage/vdisk
    ydb/core/blobstorage/base
    ydb/core/blobstorage/groupinfo
)

END()
