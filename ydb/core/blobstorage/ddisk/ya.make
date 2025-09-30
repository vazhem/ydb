LIBRARY()

SRCS(
    ddisk_actor.cpp
    ddisk_actor_impl.cpp
    ddisk_actor_mode_memory.cpp
    ddisk_actor_mode_pdisk.cpp
    ddisk_actor_mode_direct.cpp
    ddisk_actor_mode_direct_completion.cpp
    skeleton/ddisk_skeletonfront.cpp
)

PEERDIR(
    ydb/core/blobstorage/vdisk
    ydb/core/blobstorage/base
    ydb/core/blobstorage/groupinfo
    ydb/core/blobstorage/pdisk
)

END()
