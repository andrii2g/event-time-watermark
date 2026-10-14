namespace EventTimeWatermark.Core;

public static class Scenarios
{
    public static readonly string[] Names = ["ordered", "boundary", "skew", "idle-resume", "correction", "mixed"];

    public static Scenario Create(string name, uint seed = 42)
    {
        StreamEvent E(long id, int p, long t, long a, long value) => new(id, p, t * 1000, a * 1000, value);
        return name switch
        {
            "ordered" => new(name, 2, [E(1, 0, 1, 1, 5), E(2, 1, 2, 2, 7), E(3, 0, 11, 11, 3), E(4, 1, 12, 12, 4)]),
            "boundary" => new(name, 2, [E(1, 0, 9, 12, 10), E(2, 1, 11, 11, 20)]),
            "skew" => new(name, 2, [E(1, 0, 1, 1, 5), E(2, 1, 2, 2, 7), E(3, 0, 21, 21, 3), E(4, 1, 8, 22, 9), E(5, 1, 22, 23, 4)]),
            "idle-resume" => new(name, 3, [E(1, 0, 1, 1, 5), E(2, 1, 2, 2, 7), E(3, 2, 3, 3, 2), E(4, 0, 15, 15, 3), E(5, 1, 16, 16, 4), E(6, 2, 8, 22, 9), E(7, 0, 25, 25, 6), E(8, 1, 26, 26, 1), E(9, 2, 27, 27, 8)]),
            "correction" => new(name, 2, [E(1, 0, 1, 1, 5), E(2, 1, 2, 2, 7), E(3, 0, 12, 12, 3), E(4, 1, 13, 13, 4), E(5, 0, 8, 14, 9), E(6, 0, 25, 25, 6), E(7, 1, 26, 26, 1), E(8, 1, 9, 28, 11)]),
            "mixed" => Generate(seed),
            _ => throw new ArgumentException($"Unknown scenario '{name}'. Choose {string.Join(", ", Names)}.")
        };
    }

    private static Scenario Generate(uint seed)
    {
        // Fixed xorshift32 algorithm: reproducible across runtime versions.
        uint state = seed == 0 ? 0x9E3779B9u : seed;
        uint Next() { state ^= state << 13; state ^= state >> 17; state ^= state << 5; return state; }
        var events = new List<StreamEvent>();
        for (int i = 0; i < 180; i++)
        {
            int p = i % 3;
            long time = i / 3 * 1000L + Next() % 600;
            long delay = Next() % 5000;
            if (i % 17 == 0) delay += 20_000;
            long arrival = time + delay;
            if (p == 2 && time is >= 20_000 and < 35_000) arrival = Math.Max(arrival, 40_000 + i);
            events.Add(new(i + 1, p, time, arrival, 1 + Next() % 20));
        }
        return new("mixed", 3, events);
    }
}
