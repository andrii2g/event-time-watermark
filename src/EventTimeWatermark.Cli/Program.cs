using EventTimeWatermark.Core;

var scenario = Scenarios.Create(args.FirstOrDefault() ?? "boundary");
foreach (var window in Reference.Compute(scenario)) Console.WriteLine(window);
