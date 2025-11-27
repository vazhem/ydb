#include "rdma_memory_region.h"
#include <ydb/library/actors/interconnect/rdma/mem_pool.h>
#include <ydb/library/actors/util/rope.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/protos/services_common.pb.h>

namespace NActors {

std::optional<TRdmaMemoryRegion> ExtractRdmaRegion(const TRope& rope, ssize_t deviceIndex) {
    if (rope.GetSize() == 0) {
        return std::nullopt;
    }

    // Try to extract RDMA memory region from rope
    // Rope may be backed by TRcBuf which is allocated from RDMA memory pool
    auto iter = rope.Begin();
    if (!iter.Valid()) {
        return std::nullopt;
    }

    // Get the contiguous data pointer and size
    const char* data = iter.ContiguousData();
    size_t size = iter.ContiguousSize();

    if (!data || size == 0) {
        return std::nullopt;
    }

    // Check if rope is backed by a single contiguous TRcBuf
    // If rope has multiple chunks, we cannot use RDMA for it
    auto nextIter = iter;
    ++nextIter;
    if (nextIter.Valid()) {
        // Multiple chunks, cannot use RDMA directly
        // Would need to copy to single buffer first
        return std::nullopt;
    }

    // Get the TRcBuf from the iterator
    // The iterator's GetChunk() returns the underlying TRcBuf
    const TRcBuf& rcBuf = iter.GetChunk();

    // Use existing ExtractRdmaRegionFromRcBuf to get RDMA region
    auto region = ExtractRdmaRegionFromRcBuf(rcBuf, deviceIndex);

    if (region) {
        // Successfully extracted RDMA region
        return region;
    }

    // TRcBuf not RDMA-allocated (regular malloc buffer)
    return std::nullopt;
}

std::optional<TRdmaMemoryRegion> ExtractRdmaRegionFromRcBuf(const TRcBuf& rcBuf, ssize_t deviceIndex) {
    if (rcBuf.size() == 0) {
        return std::nullopt;
    }

    // Try to extract RDMA memory region from TRcBuf using existing mem_pool functions
    auto memRegionSlice = NInterconnect::NRdma::TryExtractFromRcBuf(rcBuf);
    if (memRegionSlice.Empty()) {
        return std::nullopt;
    }

    // Use existing GetAddr(), GetSize(), GetLKey(), GetRKey() methods from TMemRegionSlice
    void* addr = memRegionSlice.GetAddr();
    ui32 size = memRegionSlice.GetSize();

    if (!addr || size == 0) {
        return std::nullopt;
    }

    // Get LKEY and RKEY for the device using existing methods
    ui32 lkey = memRegionSlice.GetLKey(deviceIndex);
    ui32 rkey = memRegionSlice.GetRKey(deviceIndex);

    if (lkey == 0 || rkey == 0) {
        return std::nullopt;
    }

    // Create and return RDMA memory region
    TRdmaMemoryRegion region;
    region.Address = reinterpret_cast<ui64>(addr);
    region.Lkey = lkey;
    region.Rkey = rkey;
    region.Size = size;

    return region;
}

} // namespace NActors
