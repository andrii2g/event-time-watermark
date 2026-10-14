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
var frozen = Simulator.Run(new("all-idle", 2, [new(1, 0, 1, 1, 1), new(2, 1, 2, 2, 1)]),
    new(Strategy.PartitionWatermarks, 0, IdleTimeoutMs: 10));
Check(!frozen.Trace.Any(t => t.Kind == "watermark" && t.TimeMs >= 12), "All idle freezes progress until explicit completion");
var tied = new Scenario("tied", 2, [new(2, 1, 1, 20_000, 2), new(1, 0, 20_000, 20_000, 1)]);
Check(Simulator.Run(tied, new(Strategy.SharedWatermark, 0)).Metrics.LateEvents == 0, "Equal-time batch precedes watermark advancement");
var behindOpen = new Scenario("behind-open", 2, [new(1, 0, 5000, 5000, 1), new(2, 1, 4000, 6000, 2)]);
var behindResult = Simulator.Run(behindOpen, new(Strategy.SharedWatermark, 0));
Check(behindResult.Metrics.BehindWatermark == 1 && behindResult.Metrics.LateEvents == 0, "Behind watermark does not imply closed window");
var timer = Simulator.Run(new("timer", 2, [new(1, 0, 1, 1, 1), new(2, 1, 35_000, 35_000, 2)]), new(Strategy.ProcessingTime));
Check(timer.Emissions[0].TimeMs == 10_000, "Processing timer fires during silence");
void Reject(Action action, string message)
{
    bool rejected = false;
    try { action(); } catch (ArgumentException) { rejected = true; }
    Check(rejected, message);
}
Reject(() => (boundary with { Events = [new(1, 0, 2, 1, 1)] }).Schedule(), "Reject arrival before event time");
Reject(() => (boundary with { Events = [new(1, 0, 1, 1, 1), new(1, 1, 2, 2, 1)] }).Schedule(), "Reject duplicate IDs");
Reject(() => Simulator.Run(boundary, new(Strategy.SharedWatermark, -1)), "Reject negative delay");
Reject(() => Simulator.Run(boundary, new(Strategy.PartitionWatermarks, IdleTimeoutMs: 0)), "Reject zero idle timeout");
var reportScenario = Scenarios.Create("idle-resume");
var reportRuns = Enum.GetValues<Strategy>().Select(s => Simulator.Run(reportScenario, new(s, 3000, LatePolicy.Correct, 10_000, 8000))).ToArray();
var firstSvg = Reports.Timeline(reportScenario, reportRuns).ToString();
Check(System.Xml.Linq.XDocument.Parse(firstSvg).Root!.Name.LocalName == "svg", "Timeline parses as SVG XML");
Check(!firstSvg.Contains("NaN") && !firstSvg.Contains("Infinity"), "Finite SVG coordinates");
var originalCulture = System.Globalization.CultureInfo.CurrentCulture;
try
{
    System.Globalization.CultureInfo.CurrentCulture = new("fr-FR");
    Check(firstSvg == Reports.Timeline(reportScenario, reportRuns).ToString(), "Culture-independent SVG");
}
finally { System.Globalization.CultureInfo.CurrentCulture = originalCulture; }
string temporary = Path.Combine(Path.GetTempPath(), "watermark-tests-" + Guid.NewGuid().ToString("N"));
try
{
    Reports.Write(temporary, reportScenario, reportRuns);
    var snapshots = Directory.GetFiles(temporary).ToDictionary(path => Path.GetFileName(path)!, File.ReadAllText);
    Reports.Write(temporary, reportScenario, Enum.GetValues<Strategy>().Select(s => Simulator.Run(reportScenario, new(s, 3000, LatePolicy.Correct, 10_000, 8000))).ToArray());
    foreach (var (name, contents) in snapshots) Check(File.ReadAllText(Path.Combine(temporary, name!)) == contents, $"Byte-stable replay: {name}");
    using var json = System.Text.Json.JsonDocument.Parse(File.ReadAllText(Path.Combine(temporary, "report.json")));
    Check(json.RootElement.GetProperty("Runs").GetArrayLength() == 3, "JSON contains all strategies");
}
finally
{
    if (Directory.Exists(temporary))
    {
        foreach (string path in Directory.GetFiles(temporary)) File.Delete(path);
        Directory.Delete(temporary);
    }
}
Console.WriteLine($"Passed {assertions} assertions.");
