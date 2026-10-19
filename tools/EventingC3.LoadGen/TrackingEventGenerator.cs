using System.Buffers;
using System.Globalization;
using System.Text;
using System.Text.Json;

namespace EventingC3.LoadGen;

/// <summary>What kind of record the generator produced, so callers can compute the expected pipeline outcome.</summary>
public enum GeneratedKind
{
    Valid,
    Duplicate,
    Invalid,
}

/// <summary>
/// Produces realistic, deliberately messy tracking-event payloads: mixed field-name conventions and
/// timestamp formats, missing ids, exact duplicates (producer retries) and a mix of invalid records.
/// Deterministic for a given seed.
/// </summary>
public sealed class TrackingEventGenerator
{
    private static readonly string[] EventTypes =
        ["COLLECTED", "AT_HUB", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED", "DELIVERY_FAILED", "pod", "Hub Scan", "OFD", "RETURNED"];

    private static readonly string[] Hubs = ["JNB01", "jnb02", "PTA01", "CPT01", "DUR01", "PLZ01", "BFN01", "ELS01", "KIM01", "XYZ99"];

    private static readonly string[] Sources = ["scanner-app", "partner-api", "driver-app", "sftp-manifest"];

    private readonly Random _random;
    private readonly double _invalidRatio;
    private readonly double _duplicateRatio;
    private readonly double _missingIdRatio;
    private readonly int _waybillPool;
    private readonly DateTimeOffset _now;
    private readonly byte[][] _recent = new byte[4096][];
    private readonly ArrayBufferWriter<byte> _buffer = new(1024);
    private long _recentCount;
    private int _invalidVariant;

    public TrackingEventGenerator(int seed, double invalidRatio, double duplicateRatio, double missingIdRatio, int waybillPool = 2_000_000, DateTimeOffset? now = null)
    {
        _random = new Random(seed);
        _invalidRatio = invalidRatio;
        _duplicateRatio = duplicateRatio;
        _missingIdRatio = missingIdRatio;
        _waybillPool = waybillPool;
        _now = now ?? DateTimeOffset.UtcNow;
    }

    public long Valid { get; private set; }
    public long Duplicates { get; private set; }
    public long Invalid { get; private set; }
    public long MissingIds { get; private set; }

    /// <summary>Generates the next payload. <paramref name="key"/> is the waybill (partitioning key) when known.</summary>
    public byte[] Next(out GeneratedKind kind, out string? key)
    {
        var roll = _random.NextDouble();
        if (roll < _invalidRatio)
        {
            kind = GeneratedKind.Invalid;
            key = null;
            Invalid++;
            return NextInvalid();
        }

        if (roll < _invalidRatio + _duplicateRatio && _recentCount > 0)
        {
            kind = GeneratedKind.Duplicate;
            key = null;
            Duplicates++;
            return _recent[_random.NextInt64(Math.Min(_recentCount, _recent.Length))];
        }

        kind = GeneratedKind.Valid;
        Valid++;
        var payload = NextValid(out key);
        _recent[_recentCount++ % _recent.Length] = payload;
        return payload;
    }

    private byte[] NextValid(out string waybill)
    {
        waybill = "RAM" + _random.Next(_waybillPool).ToString("D9", CultureInfo.InvariantCulture);
        var snake = _random.Next(4) == 0;
        var occurred = _now - TimeSpan.FromSeconds(_random.Next(7 * 24 * 3600));

        _buffer.ResetWrittenCount();
        using (var json = new Utf8JsonWriter(_buffer))
        {
            json.WriteStartObject();
            if (_random.NextDouble() >= _missingIdRatio)
            {
                Span<byte> bytes = stackalloc byte[16];
                _random.NextBytes(bytes);
                json.WriteString(snake ? "event_id" : "eventId", new Guid(bytes).ToString());
            }
            else
            {
                MissingIds++;
            }

            json.WriteString(snake ? "waybill_number" : "waybill", _random.Next(10) == 0 ? waybill.ToLowerInvariant() : waybill);
            json.WriteString(snake ? "event_type" : "eventType", EventTypes[_random.Next(EventTypes.Length)]);
            switch (_random.Next(3))
            {
                case 0:
                    json.WriteString(snake ? "occurred_at" : "occurredAt", occurred.ToOffset(TimeSpan.FromHours(2)).ToString("yyyy-MM-dd'T'HH:mm:ss.fffzzz", CultureInfo.InvariantCulture));
                    break;
                case 1:
                    json.WriteString(snake ? "occurred_at" : "occurredAt", occurred.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture));
                    break;
                default:
                    json.WriteNumber(snake ? "occurred_at" : "occurredAt", occurred.ToUnixTimeMilliseconds());
                    break;
            }

            json.WriteString(snake ? "hub_code" : "hubCode", Hubs[_random.Next(Hubs.Length)]);
            json.WriteString(snake ? "courier_id" : "courierId", "C" + _random.Next(20_000).ToString("D5", CultureInfo.InvariantCulture));
            if (_random.Next(3) != 0)
            {
                json.WriteNumber("latitude", Math.Round(-22 - _random.NextDouble() * 12, 6));
                json.WriteNumber("longitude", Math.Round(17 + _random.NextDouble() * 15, 6));
            }

            json.WriteString(snake ? "source_system" : "sourceSystem", Sources[_random.Next(Sources.Length)]);
            json.WriteNumber(snake ? "schema_version" : "schemaVersion", 1);
            if (_random.Next(2) == 0)
            {
                json.WriteStartObject("attributes");
                json.WriteString("scanDevice", "TC52-" + _random.Next(5000).ToString(CultureInfo.InvariantCulture));
                json.WriteNumber("pieces", _random.Next(1, 6));
                json.WriteEndObject();
            }

            json.WriteEndObject();
        }

        return _buffer.WrittenSpan.ToArray();
    }

    private byte[] NextInvalid()
    {
        var occurred = _now.AddMinutes(-5).ToString("O", CultureInfo.InvariantCulture);
        var waybill = "RAM" + _random.Next(_waybillPool).ToString("D9", CultureInfo.InvariantCulture);
        var text = (_invalidVariant++ % 8) switch
        {
            0 => "{\"waybill\":\"" + waybill + "\",\"eventType\":\"DELIVERED\",\"occurredAt\":",          // truncated JSON
            1 => "{\"eventType\":\"DELIVERED\",\"occurredAt\":\"" + occurred + "\"}",                       // missing waybill
            2 => "{\"waybill\":\"" + waybill + "\",\"eventType\":\"!!bad!!\",\"occurredAt\":\"" + occurred + "\"}",
            3 => "{\"waybill\":\"" + waybill + "\",\"eventType\":\"AT_HUB\",\"occurredAt\":\"not-a-date\"}",
            4 => "{\"waybill\":\"" + waybill + "\",\"eventType\":\"AT_HUB\",\"occurredAt\":\"" + _now.AddDays(-400).ToString("O", CultureInfo.InvariantCulture) + "\"}",
            5 => "[1,2,3]",                                                                                 // not an object
            6 => "{\"waybill\":\"" + waybill + "\",\"eventType\":\"AT_HUB\",\"occurredAt\":\"" + occurred + "\",\"attributes\":{\"blob\":\"" + new string('x', 70_000) + "\"}}",
            _ => string.Empty,                                                                              // empty payload
        };
        return Encoding.UTF8.GetBytes(text);
    }

    /// <summary>Distinct events that should end up in the warehouse.</summary>
    public long ExpectedUniqueEvents => Valid;

    public string SummaryJson(long total, TimeSpan elapsed) => JsonSerializer.Serialize(new
    {
        total,
        valid = Valid,
        duplicates = Duplicates,
        invalid = Invalid,
        missingIds = MissingIds,
        expectedUniqueEvents = ExpectedUniqueEvents,
        elapsedSeconds = Math.Round(elapsed.TotalSeconds, 2),
        recordsPerSecond = Math.Round(total / Math.Max(elapsed.TotalSeconds, 0.001)),
    }, new JsonSerializerOptions { WriteIndented = true });
}
