using System.Threading.Tasks;

namespace ClickHouse.Client.Benchmark;

internal interface IBenchmark
{
    Task<BenchmarkResult> Run();
}
