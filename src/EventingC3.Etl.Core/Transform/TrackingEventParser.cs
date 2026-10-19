using System.Collections.Frozen;
using System.Globalization;
using System.Text.Json;
using EventingC3.Etl.Core.Domain;

namespace EventingC3.Etl.Core.Transform;

/// <summary>Outcome of parsing one raw record: either an event or a reject reason.</summary>
public readonly record struct ParseResult(TrackingEvent? Event, RejectReason Reason, string? Detail)
{
    public bool IsSuccess => Event is not null;

    public static ParseResult Ok(TrackingEvent e) => new(e, RejectReason.None, null);

    public static ParseResult Reject(RejectReason reason, string? detail = null) => new(null, reason, detail);
}

/// <summary>
/// Extract + Transform for one record: a single forward pass over the UTF-8 JSON with
/// <see cref="Utf8JsonReader"/> (no DOM), validating, normalising and enriching as it goes.
/// </summary>
/// <remarks>
/// Built for hostile input: any byte sequence yields either an event or a reject reason, never an exception.
/// Field names are matched case-insensitively and ignoring <c>_</c>/<c>-</c>, so <c>eventId</c>,
/// <c>event_id</c> and <c>EVENT-ID</c> are the same field. Unknown fields are preserved as attributes.
/// The parser is stateless and thread-safe.
/// </remarks>
public sealed class TrackingEventParser
{
    private const int MaxWaybillLength = 40;
    private const int MinWaybillLength = 4;
    private const int MaxCodeLength = 32;
    private const int MaxIdLength = 128;
    private const int MaxFieldNameLength = 64;

    private static readonly JsonReaderOptions ReaderOptions = new()
    {
        AllowTrailingCommas = true,
        CommentHandling = JsonCommentHandling.Skip,
        MaxDepth = 32,
    };

    private enum Field
    {
        Unknown,
        EventId,
        Waybill,
        EventType,
        OccurredAt,
        HubCode,
        CourierId,
        Latitude,
        Longitude,
        SourceSystem,
        SchemaVersion,
        Attributes,
    }

    private static readonly FrozenDictionary<string, Field> FieldNames = new Dictionary<string, Field>
    {
        ["eventid"] = Field.EventId,
        ["id"] = Field.EventId,
        ["waybill"] = Field.Waybill,
        ["waybillno"] = Field.Waybill,
        ["waybillnumber"] = Field.Waybill,
        ["eventtype"] = Field.EventType,
        ["type"] = Field.EventType,
        ["status"] = Field.EventType,
        ["occurredat"] = Field.OccurredAt,
        ["eventtime"] = Field.OccurredAt,
        ["timestamp"] = Field.OccurredAt,
        ["hubcode"] = Field.HubCode,
        ["hub"] = Field.HubCode,
        ["courierid"] = Field.CourierId,
        ["courier"] = Field.CourierId,
        ["latitude"] = Field.Latitude,
        ["lat"] = Field.Latitude,
        ["longitude"] = Field.Longitude,
        ["lon"] = Field.Longitude,
        ["lng"] = Field.Longitude,
        ["sourcesystem"] = Field.SourceSystem,
        ["source"] = Field.SourceSystem,
        ["schemaversion"] = Field.SchemaVersion,
        ["attributes"] = Field.Attributes,
        ["attrs"] = Field.Attributes,
    }.ToFrozenDictionary(StringComparer.Ordinal);

    private readonly TransformOptions _options;
    private readonly HubDirectory _hubs;
    private readonly TimeProvider _time;

    public TrackingEventParser(TransformOptions options, HubDirectory hubs, TimeProvider time)
    {
        _options = options;
        _hubs = hubs;
        _time = time;
    }

    /// <summary>Parses, validates, normalises and enriches one raw record.</summary>
    public ParseResult Parse(ReadOnlySpan<byte> payload, int sourcePartition = 0, long sourceOffset = 0)
    {
        if (payload.Length > _options.MaxRecordBytes)
        {
            return ParseResult.Reject(RejectReason.TooLarge, $"{payload.Length} bytes > {_options.MaxRecordBytes}");
        }

        try
        {
            return ParseCore(payload, sourcePartition, sourceOffset);
        }
        catch (JsonException ex)
        {
            return ParseResult.Reject(RejectReason.MalformedJson, ex.Message);
        }
        catch (InvalidOperationException ex)
        {
            // Utf8JsonReader throws InvalidOperationException for e.g. invalid UTF-8 inside strings.
            return ParseResult.Reject(RejectReason.MalformedJson, ex.Message);
        }
    }

    private ParseResult ParseCore(ReadOnlySpan<byte> payload, int sourcePartition, long sourceOffset)
    {
        var reader = new Utf8JsonReader(payload, ReaderOptions);
        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
        {
            return ParseResult.Reject(RejectReason.NotAnObject);
        }

        string? eventId = null, waybill = null, eventType = null, hub = null, courier = null, source = null;
        string? timestampText = null;
        long? timestampNumber = null;
        double? lat = null, lon = null;
        ushort schemaVersion = 1;
        List<KeyValuePair<string, string>>? attributes = null;

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
            {
                break;
            }

            // TokenType is PropertyName here.
            var field = Identify(ref reader, out var unknownName);
            reader.Read();

            switch (field)
            {
                case Field.EventId:
                    eventId = ReadScalar(ref reader, payload);
                    break;
                case Field.Waybill:
                    waybill = ReadScalar(ref reader, payload);
                    break;
                case Field.EventType:
                    eventType = ReadScalar(ref reader, payload);
                    break;
                case Field.HubCode:
                    hub = ReadScalar(ref reader, payload);
                    break;
                case Field.CourierId:
                    courier = ReadScalar(ref reader, payload);
                    break;
                case Field.SourceSystem:
                    source = ReadScalar(ref reader, payload);
                    break;
                case Field.OccurredAt:
                    if (reader.TokenType == JsonTokenType.Number)
                    {
                        timestampNumber = ReadEpoch(ref reader);
                        timestampText = null;
                    }
                    else
                    {
                        timestampText = ReadScalar(ref reader, payload);
                        timestampNumber = null;
                    }

                    break;
                case Field.Latitude:
                    lat = ReadDouble(ref reader);
                    break;
                case Field.Longitude:
                    lon = ReadDouble(ref reader);
                    break;
                case Field.SchemaVersion:
                    schemaVersion = ReadUInt16(ref reader);
                    break;
                case Field.Attributes:
                    if (reader.TokenType == JsonTokenType.StartObject)
                    {
                        ReadAttributes(ref reader, payload, ref attributes);
                    }
                    else
                    {
                        AddAttribute(ref attributes, "attributes", ReadScalar(ref reader, payload));
                    }

                    break;
                default:
                    // Schema drift: keep unknown fields instead of silently dropping them.
                    AddAttribute(ref attributes, unknownName, ReadScalar(ref reader, payload));
                    break;
            }
        }

        // ---- Validate + normalise -------------------------------------------------------------------------------

        if (string.IsNullOrWhiteSpace(waybill))
        {
            return ParseResult.Reject(RejectReason.MissingWaybill);
        }

        if (!TryNormalizeWaybill(waybill, out var normalizedWaybill))
        {
            return ParseResult.Reject(RejectReason.InvalidWaybill, Truncate(waybill, 64));
        }

        if (string.IsNullOrWhiteSpace(eventType))
        {
            return ParseResult.Reject(RejectReason.MissingEventType);
        }

        if (!EventTypes.TryNormalize(eventType, out var canonicalType))
        {
            return ParseResult.Reject(RejectReason.InvalidEventType, Truncate(eventType, 64));
        }

        long occurredAtMs;
        if (timestampNumber is { } epoch)
        {
            if (!TryFromEpoch(epoch, out occurredAtMs))
            {
                return ParseResult.Reject(RejectReason.InvalidTimestamp, epoch.ToString(CultureInfo.InvariantCulture));
            }
        }
        else if (string.IsNullOrWhiteSpace(timestampText))
        {
            return ParseResult.Reject(RejectReason.MissingTimestamp);
        }
        else if (!TryParseTimestamp(timestampText, _options.DefaultUtcOffset, out occurredAtMs))
        {
            return ParseResult.Reject(RejectReason.InvalidTimestamp, Truncate(timestampText, 64));
        }

        var nowMs = _time.GetUtcNow().ToUnixTimeMilliseconds();
        if (occurredAtMs < nowMs - (long)_options.MaxEventAge.TotalMilliseconds)
        {
            return ParseResult.Reject(RejectReason.TimestampTooOld);
        }

        if (occurredAtMs > nowMs + (long)_options.MaxFutureSkew.TotalMilliseconds)
        {
            return ParseResult.Reject(RejectReason.TimestampInFuture);
        }

        var hubCode = NormalizeCode(hub);
        var courierId = courier?.Trim() ?? string.Empty;
        var sourceSystem = source?.Trim() ?? string.Empty;
        if (hubCode.Length > MaxCodeLength || courierId.Length > MaxIdLength || sourceSystem.Length > MaxIdLength)
        {
            return ParseResult.Reject(RejectReason.FieldTooLong);
        }

        Guid id;
        var trimmedId = eventId?.Trim();
        if (string.IsNullOrEmpty(trimmedId))
        {
            // No producer id: derive one from the event's natural key so re-sends still deduplicate.
            id = DeterministicGuid.Create(
                DeterministicGuid.TrackingEventNamespace,
                string.Create(CultureInfo.InvariantCulture,
                    $"{normalizedWaybill}|{canonicalType}|{occurredAtMs}|{hubCode}|{courierId}"));
        }
        else if (!Guid.TryParse(trimmedId, out id))
        {
            if (trimmedId.Length > MaxIdLength)
            {
                return ParseResult.Reject(RejectReason.FieldTooLong, "eventId");
            }

            // Producer uses non-UUID ids: map them to a stable UUID and keep the original for traceability.
            id = DeterministicGuid.Create(DeterministicGuid.TrackingEventNamespace, "id:" + trimmedId);
            AddAttribute(ref attributes, "_source_event_id", trimmedId, force: true);
        }

        // Coordinates are best-effort: bad GPS must not lose the tracking event itself.
        if (lat is not { } la || lon is not { } lo || la is < -90 or > 90 || lo is < -180 or > 180 || (la == 0 && lo == 0)
            || double.IsNaN(la) || double.IsNaN(lo))
        {
            lat = null;
            lon = null;
        }

        return ParseResult.Ok(new TrackingEvent
        {
            EventId = id,
            Waybill = normalizedWaybill,
            EventType = canonicalType,
            OccurredAtUnixMs = occurredAtMs,
            HubCode = hubCode,
            Region = _hubs.RegionFor(hubCode),
            CourierId = courierId,
            Latitude = lat,
            Longitude = lon,
            SourceSystem = sourceSystem,
            SchemaVersion = schemaVersion,
            Attributes = (IReadOnlyList<KeyValuePair<string, string>>?)attributes ?? [],
            SourcePartition = sourcePartition,
            SourceOffset = sourceOffset,
        });
    }

    // ---- Field helpers ------------------------------------------------------------------------------------------

    private static Field Identify(ref Utf8JsonReader reader, out string name)
    {
        name = string.Empty;
        var length = reader.HasValueSequence ? (int)reader.ValueSequence.Length : reader.ValueSpan.Length;
        if (length > MaxFieldNameLength)
        {
            name = reader.GetString()!;
            return Field.Unknown;
        }

        Span<byte> raw = stackalloc byte[MaxFieldNameLength];
        var written = reader.CopyString(raw);
        Span<char> key = stackalloc char[written];
        var n = 0;
        for (var i = 0; i < written; i++)
        {
            var b = raw[i];
            if (b is (byte)'_' or (byte)'-')
            {
                continue;
            }

            if (b >= 0x80)
            {
                // Non-ASCII names can never be a known field.
                n = -1;
                break;
            }

            key[n++] = (char)(b is >= (byte)'A' and <= (byte)'Z' ? b + 32 : b);
        }

        if (n > 0 && FieldNames.GetAlternateLookup<ReadOnlySpan<char>>().TryGetValue(key[..n], out var field))
        {
            return field;
        }

        name = reader.GetString()!;
        return Field.Unknown;
    }

    /// <summary>Reads any JSON value as a string: strings verbatim, numbers/bools as literal text, objects/arrays as raw JSON.</summary>
    private static string? ReadScalar(ref Utf8JsonReader reader, ReadOnlySpan<byte> payload)
    {
        switch (reader.TokenType)
        {
            case JsonTokenType.String:
                return reader.GetString();
            case JsonTokenType.Number:
                return System.Text.Encoding.UTF8.GetString(reader.ValueSpan);
            case JsonTokenType.True:
                return "true";
            case JsonTokenType.False:
                return "false";
            case JsonTokenType.Null:
                return null;
            case JsonTokenType.StartObject:
            case JsonTokenType.StartArray:
                var start = (int)reader.TokenStartIndex;
                reader.Skip();
                return System.Text.Encoding.UTF8.GetString(payload[start..(int)reader.BytesConsumed]);
            default:
                return null;
        }
    }

    private static double? ReadDouble(ref Utf8JsonReader reader) => reader.TokenType switch
    {
        JsonTokenType.Number when reader.TryGetDouble(out var d) => d,
        JsonTokenType.String when double.TryParse(reader.GetString(), NumberStyles.Float, CultureInfo.InvariantCulture, out var d) => d,
        JsonTokenType.StartObject or JsonTokenType.StartArray => SkipAndReturnNull(ref reader),
        _ => null,
    };

    private static ushort ReadUInt16(ref Utf8JsonReader reader) => reader.TokenType switch
    {
        JsonTokenType.Number when reader.TryGetUInt16(out var v) => v,
        JsonTokenType.String when ushort.TryParse(reader.GetString(), NumberStyles.None, CultureInfo.InvariantCulture, out var v) => v,
        JsonTokenType.StartObject or JsonTokenType.StartArray => SkipAndReturnZero(ref reader),
        JsonTokenType.Null => 1,
        _ => 0,
    };

    /// <summary>Integer epochs keep their unit; fractional epochs (e.g. <c>1760868000.123</c> seconds) become milliseconds.</summary>
    private static long ReadEpoch(ref Utf8JsonReader reader)
    {
        if (reader.TryGetInt64(out var n))
        {
            return n;
        }

        if (reader.TryGetDouble(out var d) && d is >= 0 and < 9e15)
        {
            return d < 1e11 ? (long)(d * 1000) : (long)d;
        }

        return -1;
    }

    private static ushort SkipAndReturnZero(ref Utf8JsonReader reader)
    {
        reader.Skip();
        return 0;
    }

    private static double? SkipAndReturnNull(ref Utf8JsonReader reader)
    {
        reader.Skip();
        return null;
    }

    private void ReadAttributes(ref Utf8JsonReader reader, ReadOnlySpan<byte> payload, ref List<KeyValuePair<string, string>>? attributes)
    {
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            var key = reader.GetString()!;
            reader.Read();
            AddAttribute(ref attributes, key, ReadScalar(ref reader, payload));
        }
    }

    private void AddAttribute(ref List<KeyValuePair<string, string>>? attributes, string key, string? value, bool force = false)
    {
        if (value is null || key.Length == 0 || key.Length > _options.MaxAttributeKeyLength)
        {
            return;
        }

        attributes ??= new List<KeyValuePair<string, string>>(4);
        if (!force && attributes.Count >= _options.MaxAttributes)
        {
            return;
        }

        // Map keys must be unique; last write wins, matching JSON duplicate-key semantics.
        for (var i = 0; i < attributes.Count; i++)
        {
            if (attributes[i].Key == key)
            {
                attributes[i] = new(key, Truncate(value, _options.MaxAttributeValueLength));
                return;
            }
        }

        attributes.Add(new(key, Truncate(value, _options.MaxAttributeValueLength)));
    }

    // ---- Normalisation helpers ----------------------------------------------------------------------------------

    internal static bool TryNormalizeWaybill(string raw, out string normalized)
    {
        normalized = string.Empty;
        var span = raw.AsSpan().Trim();
        if (span.Length is < MinWaybillLength or > MaxWaybillLength)
        {
            return false;
        }

        var clean = true;
        foreach (var c in span)
        {
            if (char.IsAsciiLetterLower(c))
            {
                clean = false;
            }
            else if (!char.IsAsciiLetterUpper(c) && !char.IsAsciiDigit(c) && c is not ('-' or '_' or '/'))
            {
                return false;
            }
        }

        normalized = clean && span.Length == raw.Length ? raw : span.ToString().ToUpperInvariant();
        return true;
    }

    private static string NormalizeCode(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return string.Empty;
        }

        var span = raw.AsSpan().Trim();
        foreach (var c in span)
        {
            if (char.IsAsciiLetterLower(c))
            {
                return span.ToString().ToUpperInvariant();
            }
        }

        return span.Length == raw.Length ? raw : span.ToString();
    }

    internal static bool TryFromEpoch(long value, out long unixMs)
    {
        unixMs = value switch
        {
            < 0 => -1,
            >= 100_000_000_000_000_000 => value / 1_000_000, // nanoseconds
            >= 100_000_000_000_000 => value / 1_000,         // microseconds
            >= 100_000_000_000 => value,                     // milliseconds
            _ => value * 1_000,                              // seconds
        };
        return unixMs >= 0;
    }

    internal static bool TryParseTimestamp(string text, TimeSpan defaultOffset, out long unixMs)
    {
        unixMs = 0;
        var trimmed = text.Trim();

        if (long.TryParse(trimmed, NumberStyles.None, CultureInfo.InvariantCulture, out var epoch))
        {
            return TryFromEpoch(epoch, out unixMs);
        }

        if (HasZoneDesignator(trimmed))
        {
            if (!DateTimeOffset.TryParse(trimmed, CultureInfo.InvariantCulture, DateTimeStyles.None, out var withZone))
            {
                return false;
            }

            unixMs = withZone.ToUnixTimeMilliseconds();
            return true;
        }

        if (!DateTime.TryParse(trimmed, CultureInfo.InvariantCulture, DateTimeStyles.None, out var local))
        {
            return false;
        }

        try
        {
            unixMs = new DateTimeOffset(DateTime.SpecifyKind(local, DateTimeKind.Unspecified), defaultOffset).ToUnixTimeMilliseconds();
            return true;
        }
        catch (ArgumentOutOfRangeException)
        {
            return false;
        }
    }

    private static bool HasZoneDesignator(string s)
    {
        if (s.EndsWith("GMT", StringComparison.OrdinalIgnoreCase) || s.EndsWith("UTC", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        // Look only after the time part begins so date separators ("2026-10-19") are not mistaken for offsets.
        var timeStart = s.IndexOfAny(['T', 't', ' '], Math.Min(8, s.Length));
        if (timeStart < 0)
        {
            return false;
        }

        return s.AsSpan(timeStart + 1).IndexOfAny("Zz+-") >= 0;
    }

    private static string Truncate(string value, int max) => value.Length <= max ? value : value[..max];
}
