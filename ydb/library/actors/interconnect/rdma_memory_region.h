#pragma once

#include <ydb/library/actors/util/rope.h>
#include <ydb/library/actors/interconnect/rdma_data_transfer_events.h>
#include <optional>

namespace NActors {

////////////////////////////////////////////////////////////////////////////////
// RDMA Memory Region Detection
//
// Helper functions to detect if memory is RDMA-allocated and extract
// memory region information (address, rkey) for RDMA operations.
////////////////////////////////////////////////////////////////////////////////

// Check if TRope contains RDMA-allocated memory and extract region info
// deviceIndex: RDMA device index to extract keys for
std::optional<TRdmaMemoryRegion> ExtractRdmaRegion(const TRope& rope, ssize_t deviceIndex);

// Extract RDMA memory region from TRcBuf using existing mem_pool functions
std::optional<TRdmaMemoryRegion> ExtractRdmaRegionFromRcBuf(const TRcBuf& rcBuf, ssize_t deviceIndex);

} // namespace NActors
