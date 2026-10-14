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
Console.WriteLine($"Passed {assertions} assertions.");
