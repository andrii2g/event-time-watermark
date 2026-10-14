using EventTimeWatermark.Core;

int assertions = 0;
void Check(bool condition, string message)
{
    assertions++;
    if (!condition) throw new Exception(message);
}

Check(Windows.Start(9_999) == 0 && Windows.Start(10_000) == 10_000, "Half-open boundaries");
var boundary = Scenarios.Create("boundary");
Check(boundary.Schedule().Select(e => e.Id).SequenceEqual([2L, 1L]), "Arrival order");
Check(Reference.Compute(boundary).SequenceEqual([new Aggregate(0, 1, 10), new Aggregate(10_000, 1, 20)]), "Independent reference");
Check(Scenarios.Create("mixed").Events.SequenceEqual(Scenarios.Create("mixed").Events), "Seed replay");
Check(!Scenarios.Create("mixed", 1).Events.SequenceEqual(Scenarios.Create("mixed", 2).Events), "Seed variation");
foreach (string name in Scenarios.Names)
{
    var scenario = Scenarios.Create(name);
    Check(scenario.Schedule().Length == scenario.Events.Count, $"Schedule conservation: {name}");
    Check(Reference.Compute(scenario).Sum(w => w.Count) == scenario.Events.Count, $"Reference conservation: {name}");
}
foreach (var strategy in Enum.GetValues<Strategy>())
{
    var result = Simulator.Run(Scenarios.Create("ordered"), new(strategy));
    Check(result.FinalAccuracy.MismatchedWindows == 0, $"Ordered baseline: {strategy}");
}
var processing = Simulator.Run(boundary, new(Strategy.ProcessingTime));
Check(processing.FinalAccuracy.MismatchedWindows == 2 && processing.FinalAccuracy.AbsoluteSumError == 20,
    "Arrival-time aggregation must demonstrate wrong windows");
var shared = Simulator.Run(Scenarios.Create("skew"), new(Strategy.SharedWatermark));
var partitioned = Simulator.Run(Scenarios.Create("skew"), new(Strategy.PartitionWatermarks));
Check(shared.Metrics.DiscardedEvents == 1 && shared.FinalAccuracy.AbsoluteSumError == 9, "Fast source advances shared watermark");
Check(partitioned.Metrics.DiscardedEvents == 0 && partitioned.FinalAccuracy.MismatchedWindows == 0, "Partition minimum protects slow source");
foreach (var strategy in Enum.GetValues<Strategy>())
foreach (string name in Scenarios.Names)
{
    var result = Simulator.Run(Scenarios.Create(name), new(strategy));
    var marks = result.Trace.Where(t => t.Kind == "watermark").Select(t => t.WatermarkMs!.Value).ToArray();
    Check(marks.SequenceEqual(marks.Order()), $"Monotonic watermark: {name}/{strategy}");
    Check(result.Metrics.Events == result.Metrics.AggregateUpdates + result.Metrics.DiscardedEvents, "Event accounting");
    Check(result.Metrics.MaxBufferedEvents == 0, "Incremental aggregation requires no event buffering");
}
var correctionScenario = Scenarios.Create("correction");
var correction = Simulator.Run(correctionScenario, new(Strategy.PartitionWatermarks, 0, LatePolicy.Correct, 10_000));
Check(correction.Metrics.CorrectionEvents == 1 && correction.Metrics.ExpiredEvents == 1, "Correction and expiry");
Check(correction.InitialAccuracy.AbsoluteSumError == 20 && correction.FinalAccuracy.AbsoluteSumError == 11, "Replacement improves result");
Check(correction.Emissions.Where(e => e.WindowStartMs == 0).Select(e => e.Sum).SequenceEqual([12L, 21L]), "Versioned replacement totals");
var sideOutput = Simulator.Run(correctionScenario, new(Strategy.PartitionWatermarks, 0, LatePolicy.SideOutput));
Check(sideOutput.SideOutput.Select(x => x.Event.Id).SequenceEqual([5L, 8L]) && sideOutput.Metrics.DiscardedEvents == 0, "Side output preserves rejected records");
var idleResult = Simulator.Run(Scenarios.Create("idle-resume"), new(Strategy.PartitionWatermarks, 3000, IdleTimeoutMs: 8000));
Check(idleResult.Trace.Any(t => t.Kind == "idle" && t.PartitionId == 2 && t.TimeMs == 11_000), "Idle timeout fires without arrival");
Check(idleResult.Trace.Any(t => t.Kind == "resume" && t.PartitionId == 2 && t.TimeMs == 22_000), "Partition resumes");
Check(idleResult.Metrics.LateEvents > 0, "Idle exclusion can make returning data late");
var edge = new Scenario("expiry-boundary", 2, [new(1, 0, 1, 1, 1), new(2, 1, 20_000, 20_000, 1), new(3, 0, 2, 20_001, 1)]);
Check(Simulator.Run(edge, new(Strategy.SharedWatermark, 0, LatePolicy.Correct, 10_000)).Metrics.ExpiredEvents == 1, "Expiry equality rejects");
var within = edge with { Events = [new(1, 0, 1, 1, 1), new(2, 1, 19_999, 19_999, 1), new(3, 0, 2, 20_001, 1)] };
Check(Simulator.Run(within, new(Strategy.SharedWatermark, 0, LatePolicy.Correct, 10_000)).Metrics.CorrectionEvents == 1, "Before expiry accepts");
var unseen = new Scenario("unseen-window", 2, [new(1, 0, 12_000, 12_000, 1), new(2, 1, 1, 13_000, 2)]);
Check(Simulator.Run(unseen, new(Strategy.SharedWatermark, 0, LatePolicy.Correct)).FinalAccuracy.MismatchedWindows == 0, "Late creation of previously empty window");
for (uint seed = 1; seed <= 50; seed++)
foreach (var strategy in Enum.GetValues<Strategy>())
foreach (var policy in Enum.GetValues<LatePolicy>())
{
    var scenario = Scenarios.Create("mixed", seed);
    var result = Simulator.Run(scenario, new(strategy, 3000, policy, 10_000, 8000));
    Check(result.Metrics.Events == result.Metrics.AggregateUpdates + result.Metrics.DiscardedEvents + result.Metrics.SideOutputEvents, "Policy conservation");
    var marks = result.Trace.Where(t => t.Kind == "watermark").Select(t => t.WatermarkMs!.Value).ToArray();
    Check(marks.SequenceEqual(marks.Order()), "Watermark monotonic after resume");
    Check(result.Emissions.GroupBy(e => e.WindowStartMs).Sum(g => g.Last().Count) == result.Metrics.AggregateUpdates, "Replacement count conservation");
    var exact = Simulator.Run(scenario, new(strategy == Strategy.ProcessingTime ? Strategy.SharedWatermark : strategy, 100_000));
    Check(exact.FinalAccuracy.MismatchedWindows == 0, "Sufficient delay matches independent reference");
}
Console.WriteLine($"Passed {assertions} assertions.");
