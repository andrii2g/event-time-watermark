using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Xml.Linq;

namespace EventTimeWatermark.Core;

public sealed record ComparisonReport(string Scenario, int PartitionCount, IReadOnlyList<StreamEvent> Events,
    IReadOnlyList<Aggregate> Reference, IReadOnlyList<RunResult> Runs);

public static class Reports
{
    private static readonly JsonSerializerOptions Json = new() { WriteIndented = true, Converters = { new JsonStringEnumConverter() } };
    private static readonly JsonSerializerOptions CompactJson = new(Json) { WriteIndented = false };
    public static void Write(string directory, Scenario scenario, IReadOnlyList<RunResult> runs)
    {
        Directory.CreateDirectory(directory);
        var report = new ComparisonReport(scenario.Name, scenario.PartitionCount, scenario.Schedule(), Reference.Compute(scenario), runs);
        File.WriteAllText(Path.Combine(directory, "report.json"), JsonSerializer.Serialize(report, Json));
        File.WriteAllLines(Path.Combine(directory, "trace.jsonl"), runs.SelectMany(r => r.Trace.Select(t =>
            JsonSerializer.Serialize(new { r.Settings.Strategy, Entry = t }, CompactJson))));
        File.WriteAllLines(Path.Combine(directory, "late-events.jsonl"), runs.SelectMany(r => r.SideOutput.Select(t =>
            JsonSerializer.Serialize(new { r.Settings.Strategy, Record = t }, CompactJson))));
        Timeline(scenario, runs).Save(Path.Combine(directory, "timeline.svg"));
        WindowTable(scenario, runs).Save(Path.Combine(directory, "windows.svg"));
    }
    private static readonly XNamespace Ns = "http://www.w3.org/2000/svg";
    private static string F(double value) => value.ToString("0.###", CultureInfo.InvariantCulture);
    private static XElement E(string name, params object[] content) => new(Ns + name, content);
    private static XAttribute A(string name, double value) => new(name, F(value));
    private static XElement Canvas(int height, string title) => E("svg", new XAttribute("viewBox", $"0 0 1400 {height}"),
        new XAttribute("role", "img"), E("title", title),
        E("style", "text{font-family:Segoe UI,Arial,sans-serif;font-size:13px;fill:#dbe6f5}.title{font-size:24px;font-weight:600}.label{font-size:16px;font-weight:600}"),
        E("rect", A("width", 1400), A("height", height), new XAttribute("fill", "#101827")));
    private static XElement Text(double x, double y, string text, string css = "") => E("text", A("x", x), A("y", y), new XAttribute("class", css), text);
    private static XElement Line(double x1, double y1, double x2, double y2, string color, double width = 1) =>
        E("line", A("x1", x1), A("y1", y1), A("x2", x2), A("y2", y2), new XAttribute("stroke", color), A("stroke-width", width));

    public static XDocument Timeline(Scenario scenario, IReadOnlyList<RunResult> runs)
    {
        var svg = Canvas(125 + runs.Count * 470, $"{scenario.Name}: event arrival and watermark timelines");
        svg.Add(Text(30, 38, $"Event time / arrival time — {scenario.Name}", "title"));
        svg.Add(Text(30, 65, "Circle = event timestamp; square = arrival; connecting line = delay. Partition colors: blue / orange / purple."));
        svg.Add(Text(30, 88, "Watermarks: x = virtual processing time, y = event-time progress. Green = global. Shading = idle partition."));
        double horizon = runs.SelectMany(r => r.Trace).Max(t => t.TimeMs);
        double X(long time) => 170 + time / horizon * 1160;
        string[] colors = ["#60a5fa", "#fbbf24", "#c084fc"];
        for (int index = 0; index < runs.Count; index++)
        {
            var run = runs[index];
            int top = 120 + index * 470;
            svg.Add(Text(30, top + 10, $"{run.Settings.Strategy} | delay {run.Settings.WatermarkDelayMs}ms | {run.Settings.LatePolicy}", "label"));
            for (int p = 0; p < scenario.PartitionCount; p++)
            {
                int y = top + 50 + p * 36;
                svg.Add(Text(30, y + 5, $"Partition {p}"), Line(170, y, 1330, y, "#334155"));
                foreach (var idle in run.Trace.Where(t => t.Kind == "idle" && t.PartitionId == p))
                {
                    long end = run.Trace.FirstOrDefault(t => t.Kind == "resume" && t.PartitionId == p && t.TimeMs > idle.TimeMs)?.TimeMs ?? (long)horizon;
                    svg.Add(E("rect", A("x", X(idle.TimeMs)), A("y", y - 14), A("width", X(end) - X(idle.TimeMs)), A("height", 28),
                        new XAttribute("fill", "#64748b"), new XAttribute("opacity", ".22")));
                }
                foreach (var e in scenario.Schedule().Where(e => e.PartitionId == p))
                {
                    var group = E("g", E("title", $"Event {e.Id}: event={e.EventTimeMs}ms, arrival={e.ArrivalTimeMs}ms, value={e.Value}"));
                    group.Add(Line(X(e.EventTimeMs), y - 5, X(e.ArrivalTimeMs), y + 5, colors[p]));
                    group.Add(E("circle", A("cx", X(e.EventTimeMs)), A("cy", y - 5), A("r", 3), new XAttribute("fill", colors[p])));
                    group.Add(E("rect", A("x", X(e.ArrivalTimeMs) - 3), A("y", y + 2), A("width", 6), A("height", 6), new XAttribute("fill", colors[p])));
                    svg.Add(group);
                }
            }
            int chartTop = top + 175, chartBottom = top + 350;
            double Y(long value) => chartBottom - Math.Clamp(value / horizon, 0, 1) * (chartBottom - chartTop);
            svg.Add(Text(30, chartTop + 12, run.Settings.Strategy == Strategy.ProcessingTime ? "Clock progress" : "Watermarks"));
            for (long t = 0; t <= horizon; t += Windows.WidthMs)
            {
                svg.Add(Line(X(t), chartTop, X(t), chartBottom, "#29384c"), Text(X(t) - 8, chartBottom + 18, $"{t / 1000}s"));
                svg.Add(Line(170, Y(t), 1330, Y(t), "#29384c"), Text(125, Y(t) + 4, $"{t / 1000}s"));
            }
            void Curve(IEnumerable<TraceEntry> points, string color, int width)
            {
                var valid = points.Where(t => t.WatermarkMs is >= 0 and < long.MaxValue).ToArray();
                if (valid.Length == 0) return;
                string path = $"M {F(X(valid[0].TimeMs))} {F(Y(valid[0].WatermarkMs!.Value))}";
                foreach (var point in valid.Skip(1)) path += $" H {F(X(point.TimeMs))} V {F(Y(point.WatermarkMs!.Value))}";
                path += $" H {F(X((long)horizon))}";
                svg.Add(E("path", new XAttribute("d", path), new XAttribute("fill", "none"), new XAttribute("stroke", color), A("stroke-width", width)));
            }
            if (run.Settings.Strategy != Strategy.ProcessingTime)
                for (int p = 0; p < scenario.PartitionCount; p++) Curve(run.Trace.Where(t => t.Kind == "partition-watermark" && t.PartitionId == p), colors[p], 1);
            Curve(run.Trace.Where(t => t.Kind == "watermark"), "#34d399", 3);
            svg.Add(Text(30, top + 410, "Window output"), Line(170, top + 405, 1330, top + 405, "#334155"));
            foreach (var e in run.Emissions)
            {
                string color = e.Version > 1 || e.Kind == "late-create" ? "#f472b6" : "#34d399";
                var mark = Line(X(e.TimeMs), top + 390, X(e.TimeMs), top + 415, color, 3);
                mark.Add(E("title", $"[{e.WindowStartMs / 1000},{(e.WindowStartMs + 10_000) / 1000})s v{e.Version}: sum={e.Sum}, count={e.Count}, emitted at {e.TimeMs}ms"));
                svg.Add(mark);
            }
            svg.Add(Text(170, top + 445, $"Green = first emission; pink = late update. Completion at {F(horizon / 1000)}s flushes remaining state (terminal watermark omitted)."));
        }
        return new(svg);
    }

    public static XDocument WindowTable(Scenario scenario, IReadOnlyList<RunResult> runs)
    {
        var reference = Reference.Compute(scenario).ToDictionary(w => w.WindowStartMs);
        var starts = reference.Keys.Union(runs.SelectMany(r => r.Emissions.Select(e => e.WindowStartMs))).Order().ToArray();
        var svg = Canvas(125 + runs.Count * (90 + starts.Length * 30), $"{scenario.Name}: aggregate comparison");
        svg.Add(Text(30, 40, $"Window totals — {scenario.Name}", "title"));
        svg.Add(Text(30, 68, "Cells show sum / count. Red = differs from complete event-time reference. Missing output is shown as zero."));
        int y = 110;
        foreach (var run in runs)
        {
            svg.Add(Text(30, y, run.Settings.Strategy.ToString(), "label"));
            y += 30;
            svg.Add(Text(30, y, "Window (seconds)"), Text(300, y, "Reference"), Text(580, y, "First published"), Text(900, y, "Latest published"));
            foreach (long start in starts)
            {
                y += 30;
                var expected = reference.GetValueOrDefault(start) ?? new(start, 0, 0);
                var first = run.Emissions.FirstOrDefault(e => e.WindowStartMs == start);
                var last = run.Emissions.LastOrDefault(e => e.WindowStartMs == start);
                svg.Add(Text(30, y, $"[{start / 1000}, {(start + 10_000) / 1000})"), Text(300, y, $"{expected.Sum} / {expected.Count}"));
                foreach (var (emission, x) in new[] { (first, 580), (last, 900) })
                {
                    long sum = emission?.Sum ?? 0, count = emission?.Count ?? 0;
                    var text = Text(x, y, $"{sum} / {count}");
                    if (sum != expected.Sum || count != expected.Count) text.Add(new XAttribute("style", "fill:#fda4af"));
                    svg.Add(text);
                }
            }
            y += 60;
        }
        return new(svg);
    }
}
