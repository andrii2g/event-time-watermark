using System.Globalization;
using EventTimeWatermark.Core;

try
{
    string command = args.FirstOrDefault() ?? "demo";
    if (command is "--help" or "-h" or "help")
    {
        Console.WriteLine("""
            event-time-watermark: deterministic stream-processing simulator
            Commands: demo | compare | sweep
            Options:
              --scenario ordered|boundary|skew|idle-resume|correction|mixed
              --watermark-delay 3s       Nonnegative duration (s or ms)
              --watermark-delays 0s,2s,5s,10s   Sweep only
              --idle-timeout 8s|off      Partition strategy only; default off
              --late-policy discard|side-output|correct
              --correction-period 10s    Event-time retention after window end
              --seed 42                 Unsigned 32-bit generator seed
              --output artifacts/demo   Output directory
            All commands compare the three strategies. demo defaults to idle-resume,
            correction policy, and an 8s idle timeout. compare/sweep default to mixed.
            """);
        return 0;
    }
    if (command is not ("demo" or "compare" or "sweep")) throw new ArgumentException($"Unknown command: {command}");
    var options = new Dictionary<string, string>();
    string[] allowed = ["--scenario", "--watermark-delay", "--watermark-delays", "--idle-timeout", "--late-policy", "--correction-period", "--seed", "--output"];
    for (int i = 1; i < args.Length; i += 2)
        if (!allowed.Contains(args[i]) || i + 1 >= args.Length || !options.TryAdd(args[i], args[i + 1]))
            throw new ArgumentException($"Unknown, repeated, or incomplete option: {args[i]}");
    if (command != "sweep" && options.ContainsKey("--watermark-delays")) throw new ArgumentException("--watermark-delays requires sweep.");
    string Option(string key, string fallback) => options.GetValueOrDefault(key, fallback);
    long Duration(string value)
    {
        decimal multiplier = value.EndsWith("ms", StringComparison.Ordinal) ? 1 : value.EndsWith('s') ? 1000 : throw new ArgumentException("Durations require ms or s.");
        string digits = value[..^(multiplier == 1 ? 2 : 1)];
        if (!decimal.TryParse(digits, NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out decimal number) ||
            number < 0 || number * multiplier > 86_400_000 || decimal.Truncate(number * multiplier) != number * multiplier)
            throw new ArgumentException("Durations must be whole milliseconds between 0 and 24 hours.");
        return (long)(number * multiplier);
    }
    if (!uint.TryParse(Option("--seed", "42"), out uint seed)) throw new ArgumentException("Seed must be an unsigned 32-bit integer.");
    var scenario = Scenarios.Create(Option("--scenario", command == "demo" ? "idle-resume" : "mixed"), seed);
    var policy = Option("--late-policy", command == "demo" ? "correct" : "discard") switch
    {
        "discard" => LatePolicy.Discard, "side-output" => LatePolicy.SideOutput, "correct" => LatePolicy.Correct,
        _ => throw new ArgumentException("Unknown late policy.")
    };
    string idleValue = Option("--idle-timeout", command == "demo" ? "8s" : "off");
    long? idle = idleValue == "off" ? null : Duration(idleValue);
    long retention = Duration(Option("--correction-period", "10s"));
    long[] delays = command == "sweep" ? Option("--watermark-delays", "0s,2s,5s,10s").Split(',').Select(Duration).Distinct().ToArray()
        : [Duration(Option("--watermark-delay", "3s"))];
    string output = Path.GetFullPath(Option("--output", $"artifacts/{command}"));
    var csv = new List<string> { "delay_ms,strategy,initial_mismatches,final_mismatches,sum_error,late,discarded,corrected,max_window_states,mean_initial_latency_ms,mean_finalization_latency_ms,operations" };
    foreach (long delay in delays)
    {
        var runs = Enum.GetValues<Strategy>().Select(strategy => Simulator.Run(scenario, new(strategy, delay, policy, retention, idle))).ToArray();
        string directory = command == "sweep" ? Path.Combine(output, $"delay-{delay}ms") : output;
        Reports.Write(directory, scenario, runs);
        Console.WriteLine($"\n{scenario.Name}, delay={delay}ms, policy={policy}");
        Console.WriteLine($"{"Strategy",-22} {"Wrong windows",14} {"Sum error",10} {"Late",6} {"Discard",8} {"Correct",8} {"State peak",11}");
        foreach (var run in runs)
        {
            var m = run.Metrics;
            Console.WriteLine($"{run.Settings.Strategy,-22} {run.FinalAccuracy.MismatchedWindows,14} {run.FinalAccuracy.AbsoluteSumError,10} {m.LateEvents,6} {m.DiscardedEvents,8} {m.CorrectionEvents,8} {m.MaxWindowStates,11}");
            double Mean(IReadOnlyList<long> values) => values.Count == 0 ? 0 : values.Average();
            csv.Add(FormattableString.Invariant($"{delay},{run.Settings.Strategy},{run.InitialAccuracy.MismatchedWindows},{run.FinalAccuracy.MismatchedWindows},{run.FinalAccuracy.AbsoluteSumError},{m.LateEvents},{m.DiscardedEvents},{m.CorrectionEvents},{m.MaxWindowStates},{Mean(run.InitialLatencyMs):0.###},{Mean(run.FinalizationLatencyMs):0.###},{m.ProcessingOperations}"));
        }
        Console.WriteLine($"Reports: {directory}");
    }
    if (command == "sweep") File.WriteAllLines(Path.Combine(output, "sweep.csv"), csv);
    return 0;
}
catch (Exception ex) when (ex is ArgumentException or OverflowException or IOException or UnauthorizedAccessException)
{
    Console.Error.WriteLine($"Error: {ex.Message}");
    return 1;
}
