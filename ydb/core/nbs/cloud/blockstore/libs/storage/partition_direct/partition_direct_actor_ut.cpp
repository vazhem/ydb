#include "partition_direct_actor.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>

using namespace NKikimr;

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

// Helper function to check if a number is a power of 2
bool IsPowerOfTwo(size_t n)
{
    return n > 0 && (n & (n - 1)) == 0;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartitionDirectActorTest)
{
    Y_UNIT_TEST(GetNumDirectBlockGroups)
    {
        // Map of IOPS to expected number of direct block groups
        THashMap<ui32, size_t> testCases = {
            {0, 32},     {1, 1},      {500, 1},    {999, 1},     {1000, 1},
            {1001, 2},   {1500, 2},   {2000, 2},   {2001, 4},    {3000, 4},
            {4000, 4},   {4001, 8},   {5000, 8},   {8000, 8},    {8001, 16},
            {10000, 16}, {16000, 16}, {16001, 32}, {20000, 32},  {32000, 32},
            {32001, 64}, {40000, 64}, {64000, 64}, {64001, 128}, {100000, 128},
        };

        for (const auto& [iops, expected]: testCases) {
            size_t result = CalculateNumDirectBlockGroupsFromIops(iops);

            UNIT_ASSERT_VALUES_EQUAL_C(
                expected,
                result,
                TStringBuilder() << "For IOPS=" << iops << ", expected "
                                 << expected << " but got " << result);

            UNIT_ASSERT_C(
                IsPowerOfTwo(result),
                TStringBuilder() << "Result " << result << " for IOPS " << iops
                                 << " is not a power of 2");
        }
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
