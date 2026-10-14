namespace EventTimeWatermark.Core;

public enum Strategy { ProcessingTime, SharedWatermark, PartitionWatermarks }
public enum LatePolicy { Discard, SideOutput, Correct }
public sealed record Settings(Strategy Strategy, long WatermarkDelayMs = 3000,
    LatePolicy LatePolicy = LatePolicy.Discard, long CorrectionPeriodMs = 10_000, long? IdleTimeoutMs = null);
public sealed record TraceEntry(long TimeMs, string Kind, long? EventId = null, int? PartitionId = null,
    long? WatermarkMs = null, long? WindowStartMs = null, string? Detail = null);
public sealed record Emission(long TimeMs, long WindowStartMs, int Version, long Count, long Sum, string Kind);
public sealed record LateRecord(StreamEvent Event, string Reason);
public sealed record Accuracy(int MismatchedWindows, long AbsoluteSumError, long AbsoluteCountError);
public sealed class Metrics
{
    public long Events { get; set; }
    public long BehindWatermark { get; set; }
    public long LateEvents { get; set; }
    public long DiscardedEvents { get; set; }
    public long SideOutputEvents { get; set; }
    public long CorrectionEvents { get; set; }
    public long CorrectedWindows { get; set; }
    public long ExpiredEvents { get; set; }
    public int MaxOpenWindows { get; set; }
    public int MaxRetainedCorrectionWindows { get; set; }
    public int MaxWindowStates { get; set; }
    public int MaxBufferedEvents { get; set; }
    public long AggregateUpdates { get; set; }
    public long WatermarkEvaluations { get; set; }
    public long WindowInspections { get; set; }
    public long Emissions { get; set; }
    public long ProcessingOperations => Events + AggregateUpdates + WatermarkEvaluations + WindowInspections + Emissions;
}
public sealed record RunResult(Settings Settings, Metrics Metrics, Accuracy InitialAccuracy, Accuracy FinalAccuracy,
    IReadOnlyList<Emission> Emissions, IReadOnlyList<TraceEntry> Trace, IReadOnlyList<LateRecord> SideOutput,
    IReadOnlyList<long> InitialLatencyMs, IReadOnlyList<long> FinalizationLatencyMs);

public static class Simulator
{
    private sealed class WindowState
    {
        public long Count;
        public long Sum;
        public int Version;
    }

    public static RunResult Run(Scenario scenario, Settings settings)
    {
        if (settings.WatermarkDelayMs < 0 || settings.CorrectionPeriodMs < 0 || settings.IdleTimeoutMs <= 0)
            throw new ArgumentException("Delays must be nonnegative; idle timeout must be positive.");
        if (!Enum.IsDefined(settings.Strategy) || !Enum.IsDefined(settings.LatePolicy))
            throw new ArgumentException("Unknown strategy or late policy.");
        var input = scenario.Schedule();
        var windows = new SortedDictionary<long, WindowState>();
        var emissions = new List<Emission>();
        var trace = new List<TraceEntry>();
        var side = new List<LateRecord>();
        var initialLatency = new List<long>();
        var finalLatency = new List<long>();
        var metrics = new Metrics();
        var maxima = Enumerable.Repeat(long.MinValue, scenario.PartitionCount).ToArray();
        var lastArrival = new long[scenario.PartitionCount];
        var idle = new bool[scenario.PartitionCount];
        var corrected = new HashSet<long>();
        long watermark = long.MinValue;
        long now = 0;

        void Measure()
        {
            metrics.MaxOpenWindows = Math.Max(metrics.MaxOpenWindows, windows.Count(w => w.Value.Version == 0));
            metrics.MaxRetainedCorrectionWindows = Math.Max(metrics.MaxRetainedCorrectionWindows, windows.Count(w => w.Value.Version > 0));
            metrics.MaxWindowStates = Math.Max(metrics.MaxWindowStates, windows.Count);
        }
        void Emit(long start, WindowState state, string kind)
        {
            state.Version++;
            emissions.Add(new(now, start, state.Version, state.Count, state.Sum, kind));
            trace.Add(new(now, kind, WindowStartMs: start));
            metrics.Emissions++;
            if (state.Version == 1) initialLatency.Add(now - (start + Windows.WidthMs));
        }
        void CloseWindows()
        {
            foreach (var (start, state) in windows.ToArray())
            {
                metrics.WindowInspections++;
                if (watermark < start + Windows.WidthMs) continue;
                if (state.Version == 0) Emit(start, state, "emit");
                if (settings.Strategy != Strategy.ProcessingTime && settings.LatePolicy == LatePolicy.Correct &&
                    watermark < start + Windows.WidthMs + settings.CorrectionPeriodMs) continue;
                finalLatency.Add(now - (start + Windows.WidthMs));
                trace.Add(new(now, "finalize", WindowStartMs: start));
                windows.Remove(start);
            }
            Measure();
        }
        void Advance(long candidate)
        {
            metrics.WatermarkEvaluations++;
            if (candidate <= watermark) return;
            watermark = candidate;
            trace.Add(new(now, candidate == long.MaxValue ? "complete" : "watermark", WatermarkMs: candidate));
            CloseWindows();
        }

        // Processing-time timers fire before arrivals at their boundary. Event-time
        // progress is evaluated after all arrivals at an equal timestamp.
        long horizon = Windows.Start(input.Max(e => e.ArrivalTimeMs)) + Windows.WidthMs;
        var batches = input.GroupBy(e => e.ArrivalTimeMs).ToDictionary(g => g.Key, g => g.ToArray());
        var ticks = new SortedSet<long>(batches.Keys);
        if (settings.IdleTimeoutMs is long timeout && settings.Strategy == Strategy.PartitionWatermarks)
        {
            if (timeout < horizon) ticks.Add(timeout);
            foreach (var e in input)
                if (e.ArrivalTimeMs + timeout < horizon) ticks.Add(e.ArrivalTimeMs + timeout);
        }
        foreach (long tick in ticks)
        {
            if (settings.Strategy == Strategy.ProcessingTime)
            {
                foreach (long end in windows.Keys.Select(s => s + Windows.WidthMs).Where(t => t <= tick).ToArray())
                { now = end; Advance(end); }
            }
            now = tick;
            foreach (var e in batches.GetValueOrDefault(tick) ?? [])
            {
                metrics.Events++;
                trace.Add(new(now, "arrival", e.Id, e.PartitionId));
                lastArrival[e.PartitionId] = now;
                if (idle[e.PartitionId]) trace.Add(new(now, "resume", PartitionId: e.PartitionId));
                idle[e.PartitionId] = false;
                if (settings.Strategy != Strategy.ProcessingTime && e.EventTimeMs < watermark) metrics.BehindWatermark++;
                long start = Windows.Start(settings.Strategy == Strategy.ProcessingTime ? now : e.EventTimeMs);
                if (start + Windows.WidthMs <= watermark)
                {
                    metrics.LateEvents++;
                    if (settings.LatePolicy == LatePolicy.Correct && watermark < start + Windows.WidthMs + settings.CorrectionPeriodMs)
                    {
                        if (!windows.TryGetValue(start, out var state)) windows.Add(start, state = new());
                        state.Count++;
                        state.Sum += e.Value;
                        metrics.AggregateUpdates++;
                        metrics.CorrectionEvents++;
                        corrected.Add(start);
                        metrics.CorrectedWindows = corrected.Count;
                        Emit(start, state, state.Version == 0 ? "late-create" : "correct");
                        Measure();
                    }
                    else if (settings.LatePolicy == LatePolicy.SideOutput)
                    {
                        metrics.SideOutputEvents++;
                        side.Add(new(e, "Window already emitted"));
                        trace.Add(new(now, "side-output", e.Id, e.PartitionId, WindowStartMs: start));
                    }
                    else
                    {
                        metrics.DiscardedEvents++;
                        if (settings.LatePolicy == LatePolicy.Correct) metrics.ExpiredEvents++;
                        trace.Add(new(now, "discard", e.Id, e.PartitionId, WindowStartMs: start,
                            Detail: settings.LatePolicy == LatePolicy.Correct ? "Correction period expired" : "Window already emitted"));
                    }
                }
                else
                {
                    if (!windows.TryGetValue(start, out var state)) windows.Add(start, state = new());
                    state.Count++;
                    state.Sum += e.Value;
                    metrics.AggregateUpdates++;
                    Measure();
                }
                maxima[e.PartitionId] = Math.Max(maxima[e.PartitionId], e.EventTimeMs);
            }
            if (settings.Strategy != Strategy.ProcessingTime)
            {
                if (settings.Strategy == Strategy.PartitionWatermarks && settings.IdleTimeoutMs is long idleTimeout)
                    for (int p = 0; p < idle.Length; p++)
                        if (!idle[p] && now - lastArrival[p] >= idleTimeout)
                        {
                            idle[p] = true;
                            trace.Add(new(now, "idle", PartitionId: p));
                        }
                for (int p = 0; p < maxima.Length; p++)
                    trace.Add(new(now, "partition-watermark", PartitionId: p,
                        WatermarkMs: maxima[p] == long.MinValue ? long.MinValue : maxima[p] - settings.WatermarkDelayMs));
                var active = maxima.Where((_, p) => !idle[p]).ToArray();
                if (active.Length == 0) continue; // Idleness alone is not completion.
                long progress = settings.Strategy == Strategy.SharedWatermark ? maxima.Max() : active.Min();
                Advance(progress == long.MinValue ? progress : progress - settings.WatermarkDelayMs);
            }
        }
        // Explicit common source-completion horizon; no inferred early completion
        // of a temporarily quiet partition using future dataset knowledge.
        now = horizon;
        for (int p = 0; p < scenario.PartitionCount; p++) trace.Add(new(now, "source-complete", PartitionId: p));
        Advance(long.MaxValue);
        var reference = Reference.Compute(scenario);
        return new(settings, metrics, Compare(reference, emissions, false), Compare(reference, emissions, true),
            emissions, trace, side, initialLatency, finalLatency);
    }

    public static Accuracy Compare(IEnumerable<Aggregate> reference, IEnumerable<Emission> emissions, bool latest)
    {
        var expected = reference.ToDictionary(w => w.WindowStartMs);
        var actual = emissions.GroupBy(e => e.WindowStartMs).ToDictionary(g => g.Key, g => latest ? g.Last() : g.First());
        int mismatches = 0;
        long sumError = 0, countError = 0;
        foreach (long start in expected.Keys.Union(actual.Keys))
        {
            expected.TryGetValue(start, out var r);
            actual.TryGetValue(start, out var a);
            long sum = Math.Abs((r?.Sum ?? 0) - (a?.Sum ?? 0));
            long count = Math.Abs((r?.Count ?? 0) - (a?.Count ?? 0));
            if (sum != 0 || count != 0) mismatches++;
            sumError += sum;
            countError += count;
        }
        return new(mismatches, sumError, countError);
    }
}
