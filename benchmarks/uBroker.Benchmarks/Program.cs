using BenchmarkDotNet.Running;
using uBroker.Benchmarks;

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).RunAll();
