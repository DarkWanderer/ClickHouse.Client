using System.Threading.Tasks;
using BenchmarkDotNet.Running;

namespace ClickHouse.Client.Benchmark;

internal class Program
{
    public static async Task Main(string[] args) => BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
}
