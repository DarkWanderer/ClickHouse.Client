using System;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;

namespace ClickHouse.Client.Benchmark.Benchmarks;

internal abstract class AbstractParameterizedBenchmark : IBenchmark
{
    private readonly ClickHouseConnectionStringBuilder connectionStringBuilder = new ClickHouseConnectionStringBuilder();

    internal AbstractParameterizedBenchmark(string connectionString)
    {
        Duration = TimeSpan.FromSeconds(20);
        connectionStringBuilder.ConnectionString = connectionString;
    }

    public TimeSpan Duration { get; set; }

    public abstract Task<BenchmarkResult> Run();

    protected bool Compression { get => connectionStringBuilder.Compression; set => connectionStringBuilder.Compression = value; }

    protected ClickHouseConnection GetConnection() => new ClickHouseConnection(connectionStringBuilder.ToString());
}
