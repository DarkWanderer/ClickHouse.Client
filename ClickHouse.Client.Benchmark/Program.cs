using BenchmarkDotNet.Running;

namespace ClickHouse.Client.Benchmark;

internal class Program
{
    public static void Main(string[] args) => BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
}
