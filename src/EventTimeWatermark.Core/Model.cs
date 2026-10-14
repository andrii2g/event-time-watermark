namespace EventTimeWatermark.Core;

public sealed record StreamEvent(long Id, int PartitionId, long EventTimeMs, long ArrivalTimeMs, long Value);
public sealed record Aggregate(long WindowStartMs, long Count, long Sum);
public sealed record Scenario(string Name, int PartitionCount, IReadOnlyList<StreamEvent> Events)
{
    public StreamEvent[] Schedule()
    {
        if (PartitionCount is < 2 or > 3) throw new ArgumentException("Use two or three partitions.");
        if (Events.Count == 0) throw new ArgumentException("A scenario must contain events.");
        if (Events.Select(e => e.Id).Distinct().Count() != Events.Count)
            throw new ArgumentException("Event IDs must be unique.");
        if (Events.Any(e => e.EventTimeMs < 0 || e.ArrivalTimeMs < e.EventTimeMs ||
                            e.PartitionId < 0 || e.PartitionId >= PartitionCount))
            throw new ArgumentException("Invalid timestamp or partition.");
        return Events.OrderBy(e => e.ArrivalTimeMs).ThenBy(e => e.Id).ToArray();
    }
}

public static class Windows
{
    public const long WidthMs = 10_000;
    public static long Start(long timestamp) => timestamp / WidthMs * WidthMs;
}

// Offline reference deliberately does not call the streaming window assigner.
public static class Reference
{
    public static Aggregate[] Compute(Scenario scenario)
    {
        _ = scenario.Schedule();
        return scenario.Events.GroupBy(e => e.EventTimeMs - e.EventTimeMs % 10_000)
            .OrderBy(g => g.Key)
            .Select(g => new Aggregate(g.Key, g.LongCount(), g.Sum(e => e.Value))).ToArray();
    }
}
