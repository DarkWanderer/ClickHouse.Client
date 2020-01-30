using System.Threading.Tasks;

namespace ClickHouse.Client.Benchmark
{
    internal interface IBenchmark
    {
        string Name { get; }

        Task<BenchmarkResult> Run();
    }
}
