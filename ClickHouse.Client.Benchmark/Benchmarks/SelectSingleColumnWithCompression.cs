namespace ClickHouse.Client.Benchmark.Benchmarks
{
    internal class SelectSingleColumnWithCompression : SelectSingleColumnWithoutCompression, IBenchmark
    {
        public SelectSingleColumnWithCompression(string connectionString) : base(connectionString)
        {
        }

        public override string Name => "Select single column with compression";

        protected override bool Compression => true;
    }
}
