namespace ClickHouse.Client.Benchmark.Benchmarks
{
    internal class BulkWriteSingleColumnWithCompression : BulkWriteSingleColumnWithoutCompression
    {
        public BulkWriteSingleColumnWithCompression(string connectionString) : base(connectionString)
        {
            Compression = true;
        }
    }
}
