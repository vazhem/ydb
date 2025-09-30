LIBRARY()

SRCS(
    ddisk_actor.cpp
    ddisk_actor_impl.cpp
    skeleton/ddisk_skeletonfront.cpp
)

PEERDIR(
    ydb/core/blobstorage/vdisk
    ydb/core/blobstorage/base
    ydb/core/blobstorage/groupinfo
)

END()
