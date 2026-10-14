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
Console.WriteLine($"Passed {assertions} assertions.");
