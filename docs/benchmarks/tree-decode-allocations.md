# Decoded B-tree page allocations

A full 4096-byte page previously grew its decoded entry slice repeatedly. The decoder now reserves exactly the validated entry count after CRC, identity, kind, level and capacity checks. Empty leaves retain their nil entry slice. Raw mmap lookup and the storage format are unchanged.

Measured on Linux amd64, Ryzen9 7900X3D, Go benchmark default24 logical processors. Same benchmark and workload, baseline7f822be; three300ms samples per shape, median shown. These are isolated decoder measurements, not VM boot or disk throughput.

| Page | Allocations before / after | Allocated bytes before / after | Median ns before / after |
| --- | ---: | ---: | ---: |
| Object leaf |8 /1 |21,424 /6,144 |4,583 /2,535 |
| Object internal |8 /1 |21,424 /6,144 |4,485 /2,468 |
| Free leaf |9 /1 |43,184 /20,480 |12,131 /8,262 |
| Free internal |8 /1 |21,424 /10,240 |6,200 /4,166 |

Reproduce with `go test ./volume -run '^$' -bench '^BenchmarkDecodeTreePage$' -benchmem -count=3 -benchtime=300ms`. The allocation regression test fails on the original decoder. Page roundtrips and malformed count, checksum and duplicate-key cases run under the race detector alongside existing tree safety tests.
