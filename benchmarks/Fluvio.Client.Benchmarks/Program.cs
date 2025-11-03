using BenchmarkDotNet.Running;

namespace Fluvio.Client.Benchmarks;

class Program
{
    static void Main(string[] args)
    {
        // Run all benchmarks or specific ones based on args
        var summary = BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }
}
