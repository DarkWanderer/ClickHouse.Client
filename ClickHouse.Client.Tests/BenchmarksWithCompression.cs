using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    [NonParallelizable]
    [Explicit]
    [Category("Benchmark")]
    public class BenchmarksWithCompression : BenchmarksWithoutCompression
    {
        protected override bool UseCompression => true;
    }
}
