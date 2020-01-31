using System;

namespace ClickHouse.Client.Benchmark
{
    struct BenchmarkResult
    {
        public TimeSpan Duration;
        public ulong DataSize;
        public ulong RowsCount;

        public double DataThroughput => DataSize / Duration.TotalMilliseconds * 1000;

        public double RowsThroughput => RowsCount / Duration.TotalMilliseconds * 1000;
    }
}
