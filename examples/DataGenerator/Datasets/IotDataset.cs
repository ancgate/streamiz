namespace Streamiz.Examples.DataGenerator.Datasets;

/// <summary>
/// 50 sensors reporting every minute for 4 hours (2026-01-05 08:00 to 12:00 UTC), in the order the readings
/// arrive (not the order they were measured): with anomalies, silent devices, out-of-order and late readings.
/// </summary>
public static class IotDataset
{
    public static readonly DateTime Start = Json.Utc(2026, 1, 5, 8);
    public const int Minutes = 240;
    public const int DeviceCount = 50;

    private static readonly (string Location, double Base)[] Locations =
        { ("warehouse-a", 18), ("warehouse-b", 16), ("office", 21), ("cold-room", 4) };

    private static readonly string[] DeviceModels = { "TH-100", "TH-200", "EnviroSense-3" };

    // Patterns embedded on purpose (documented in data/README.md and facts.json)
    public static readonly (string DeviceId, int Minute, double Delta)[] Spikes =
    {
        ("D07", 90, 15), ("D07", 91, 15), ("D07", 92, 15), ("D23", 150, -12)
    };

    public static readonly (string DeviceId, int FromMinute, int ToMinute)[] Silences =
    {
        ("D13", 121, Minutes), // stops reporting at 10:01 and never comes back
        ("D31", 60, 100),      // silent from 09:00 to 09:40, then reports again
    };

    public const string LateDevice = "D42";
    public const int LateFromMinute = 180;
    public const int LateToMinute = 190;
    public static readonly TimeSpan LateDelay = TimeSpan.FromMinutes(15);

    public record Data(List<Device> Devices, List<Reading> ReadingsInArrivalOrder, int OutOfOrder);

    public static Data Generate(int seed)
    {
        var rng = new Rng(seed);
        var devices = new List<Device>();
        var baseTemperature = new Dictionary<string, double>();
        for (var d = 1; d <= DeviceCount; ++d)
        {
            var id = $"D{d:D2}";
            var (location, temperature) = Locations[(d - 1) % Locations.Length];
            devices.Add(new Device(id, location, rng.Pick(DeviceModels), Json.Utc(2025, 6, 1).AddDays(d)));
            baseTemperature[id] = temperature + rng.Double(-1.5, 1.5);
        }

        var arrivals = new List<(DateTime Arrival, int Order, Reading Reading)>();
        var outOfOrder = 0;
        var order = 0;
        foreach (var device in devices)
        {
            var battery = device.DeviceId == "D44" ? 12.0 : rng.Double(60, 100);
            var humidity = rng.Double(35, 55);
            var offset = TimeSpan.FromSeconds(int.Parse(device.DeviceId[1..]));
            for (var minute = 0; minute < Minutes; ++minute)
            {
                battery -= 0.02;
                humidity = Math.Clamp(humidity + rng.Gaussian() * 0.4, 20, 80);
                if (Silences.Any(s => s.DeviceId == device.DeviceId && minute >= s.FromMinute && minute < s.ToMinute))
                    continue;

                var measuredAt = Start.AddMinutes(minute) + offset;
                // a slow daily cycle plus sensor noise
                var temperature = baseTemperature[device.DeviceId] + Math.Sin(minute / 240.0 * Math.PI) * 1.5 + rng.Gaussian() * 0.3;
                temperature += Spikes.Where(s => s.DeviceId == device.DeviceId && s.Minute == minute).Sum(s => s.Delta);

                var arrival = measuredAt + TimeSpan.FromMilliseconds(rng.Int(100, 2000));
                if (device.DeviceId == LateDevice && minute >= LateFromMinute && minute < LateToMinute)
                    arrival = measuredAt + LateDelay; // buffered by a gateway that lost its connection
                else if (rng.Chance(0.01))
                {
                    arrival = measuredAt + rng.Seconds(30, 90); // network hiccup: arrives after newer readings
                    outOfOrder++;
                }

                arrivals.Add((arrival, order++, new Reading(device.DeviceId, Math.Round(temperature, 2),
                    Math.Round(humidity, 1), (int)Math.Max(0, Math.Round(battery)), measuredAt)));
            }
        }

        var readings = arrivals.OrderBy(a => a.Arrival).ThenBy(a => a.Order).Select(a => a.Reading).ToList();
        return new Data(devices, readings, outOfOrder);
    }

    public static void Write(string dataDir, int seed)
    {
        var data = Generate(seed);
        var dir = Path.Combine(dataDir, "iot");
        Json.WriteLines(Path.Combine(dir, "devices.jsonl"), data.Devices);
        // the readings are in arrival order, split in 2 files: part 2 follows part 1
        var half = data.ReadingsInArrivalOrder.Count / 2;
        Json.WriteLines(Path.Combine(dir, "readings-part1.jsonl"), data.ReadingsInArrivalOrder.Take(half));
        Json.WriteLines(Path.Combine(dir, "readings-part2.jsonl"), data.ReadingsInArrivalOrder.Skip(half));

        Json.WriteFacts(Path.Combine(dir, "facts.json"), new
        {
            seed,
            devices = DeviceCount,
            readings = data.ReadingsInArrivalOrder.Count,
            from = Start,
            to = Start.AddMinutes(Minutes),
            readingIntervalSeconds = 60,
            spikes = Spikes.Select(s => new { deviceId = s.DeviceId, measuredAt = Start.AddMinutes(s.Minute).AddSeconds(int.Parse(s.DeviceId[1..])), delta = s.Delta }),
            silences = Silences.Select(s => new
            {
                deviceId = s.DeviceId,
                silentFrom = Start.AddMinutes(s.FromMinute),
                silentUntil = s.ToMinute >= Minutes ? (DateTime?)null : Start.AddMinutes(s.ToMinute)
            }),
            lateReadings = new
            {
                deviceId = LateDevice,
                count = LateToMinute - LateFromMinute,
                measuredFrom = Start.AddMinutes(LateFromMinute),
                delayMinutes = LateDelay.TotalMinutes
            },
            outOfOrderReadings = data.OutOfOrder,
            lowBatteryDevice = "D44"
        });
    }
}
