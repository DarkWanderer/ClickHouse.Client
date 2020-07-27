namespace ClickHouse.Client.Benchmark.Benchmarks
{

    internal class SelectSingleColumnWithCompression : SelectSingleColumnWithoutCompression, IBenchmark
    {
        public SelectSingleColumnWithCompression(string connectionString) : base(connectionString)
        {
            Compression = true;
        }
    }
}
