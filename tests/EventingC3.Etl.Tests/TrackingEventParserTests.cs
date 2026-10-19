using System.Text;
using EventingC3.Etl.Core.Domain;
using EventingC3.Etl.Core.Transform;
using EventingC3.Etl.TestSupport;

namespace EventingC3.Etl.Tests;

public class TrackingEventParserTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 19, 12, 0, 0, TimeSpan.Zero);
    private readonly TrackingEventParser _parser = new(new TransformOptions(), HubDirectory.Default, new FixedTimeProvider(Now));

    private ParseResult Parse(string json) => _parser.Parse(Encoding.UTF8.GetBytes(json), 3, 42);

    private TrackingEvent ParseOk(string json)
    {
        var result = Parse(json);
        Assert.True(result.IsSuccess, $"Expected success but got {result.Reason}: {result.Detail}");
        return result.Event!;
    }

    [Fact]
    public void Parses_a_complete_event_and_enriches_it()
    {
        var e = ParseOk("""
            {"eventId":"0f8fad5b-d9cb-469f-a165-70867728950e","waybill":"RAM000000123","eventType":"DELIVERED",
             "occurredAt":"2026-10-19T10:15:00.250+02:00","hubCode":"jnb01","courierId":" C123 ","latitude":-26.2041,
             "longitude":28.0473,"sourceSystem":"scanner-app","schemaVersion":2,"attributes":{"pieces":3,"signedBy":"T. Nkosi"}}
            """);

        Assert.Equal(Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e"), e.EventId);
        Assert.Equal("RAM000000123", e.Waybill);
        Assert.Equal(EventTypes.Delivered, e.EventType);
        Assert.Equal(new DateTimeOffset(2026, 10, 19, 8, 15, 0, 250, TimeSpan.Zero).ToUnixTimeMilliseconds(), e.OccurredAtUnixMs);
        Assert.Equal("JNB01", e.HubCode);
        Assert.Equal("GAUTENG", e.Region);
        Assert.Equal("C123", e.CourierId);
        Assert.Equal(-26.2041, e.Latitude);
        Assert.Equal(28.0473, e.Longitude);
        Assert.Equal("scanner-app", e.SourceSystem);
        Assert.Equal((ushort)2, e.SchemaVersion);
        Assert.Equal([new("pieces", "3"), new("signedBy", "T. Nkosi")], e.Attributes);
        Assert.Equal(3, e.SourcePartition);
        Assert.Equal(42, e.SourceOffset);
    }

    [Fact]
    public void Accepts_snake_case_kebab_case_and_aliases()
    {
        var e = ParseOk("""{"event_id":"0f8fad5b-d9cb-469f-a165-70867728950e","WAYBILL-NUMBER":"ram123456","type":"pod","event_time":"2026-10-19T09:00:00Z","hub":"CPT01","lat":"-33.9","lng":"18.4"}""");
        Assert.Equal("RAM123456", e.Waybill);
        Assert.Equal(EventTypes.Delivered, e.EventType);
        Assert.Equal("WESTERN_CAPE", e.Region);
        Assert.Equal(-33.9, e.Latitude);
        Assert.Equal(18.4, e.Longitude);
    }

    [Theory]
    [InlineData("pod", "DELIVERED")]
    [InlineData("Hub Scan", "AT_HUB")]
    [InlineData("out-for-delivery", "OUT_FOR_DELIVERY")]
    [InlineData("OFD", "OUT_FOR_DELIVERY")]
    [InlineData("customs hold", "CUSTOMS_HOLD")] // unknown but well-formed: kept, never lost
    public void Normalises_event_types(string raw, string expected)
    {
        var e = ParseOk($$"""{"waybill":"RAM123456","eventType":"{{raw}}","occurredAt":"2026-10-19T09:00:00Z"}""");
        Assert.Equal(expected, e.EventType);
    }

    [Theory]
    [InlineData("\"2026-10-19T10:00:00Z\"", "2026-10-19T10:00:00Z")]
    [InlineData("\"2026-10-19T12:00:00+02:00\"", "2026-10-19T10:00:00Z")]
    [InlineData("\"2026-10-19T12:00:00\"", "2026-10-19T10:00:00Z")]          // no zone: SAST assumed
    [InlineData("\"2026-10-19 12:00:00\"", "2026-10-19T10:00:00Z")]
    [InlineData("1760868000", "2025-10-19T10:00:00Z")]                         // epoch seconds
    public void Parses_timestamp_formats(string jsonValue, string expectedUtc)
    {
        var parser = new TrackingEventParser(new TransformOptions { MaxEventAge = TimeSpan.FromDays(3650) }, HubDirectory.Default, new FixedTimeProvider(Now));
        var result = parser.Parse(Encoding.UTF8.GetBytes($$"""{"waybill":"RAM123456","eventType":"AT_HUB","occurredAt":{{jsonValue}}}"""));
        Assert.True(result.IsSuccess, result.Reason.ToString());
        Assert.Equal(DateTimeOffset.Parse(expectedUtc).ToUnixTimeMilliseconds(), result.Event!.OccurredAtUnixMs);
    }

    [Theory]
    [InlineData(1_792_404_000L, 1_792_404_000_000L)]                 // seconds
    [InlineData(1_792_404_000_123L, 1_792_404_000_123L)]             // milliseconds
    [InlineData(1_792_404_000_123_456L, 1_792_404_000_123L)]         // microseconds
    [InlineData(1_792_404_000_123_456_789L, 1_792_404_000_123L)]     // nanoseconds
    public void Detects_epoch_units(long epoch, long expectedMs)
    {
        Assert.True(TrackingEventParser.TryFromEpoch(epoch, out var ms));
        Assert.Equal(expectedMs, ms);
    }

    [Fact]
    public void Accepts_fractional_epoch_seconds_and_numeric_strings()
    {
        var expected = Now.AddHours(-1).ToUnixTimeMilliseconds();
        var seconds = (expected / 1000.0).ToString("F3", System.Globalization.CultureInfo.InvariantCulture);
        Assert.Equal(expected, ParseOk($$"""{"waybill":"RAM123456","eventType":"AT_HUB","occurredAt":{{seconds}}}""").OccurredAtUnixMs);
        Assert.Equal(expected, ParseOk($$"""{"waybill":"RAM123456","eventType":"AT_HUB","occurredAt":"{{expected}}"}""").OccurredAtUnixMs);
    }

    [Theory]
    [InlineData("""{"eventType":"AT_HUB","occurredAt":"2026-10-19T09:00:00Z"}""", RejectReason.MissingWaybill)]
    [InlineData("""{"waybill":"  ","eventType":"AT_HUB","occurredAt":"2026-10-19T09:00:00Z"}""", RejectReason.MissingWaybill)]
    [InlineData("""{"waybill":"RA","eventType":"AT_HUB","occurredAt":"2026-10-19T09:00:00Z"}""", RejectReason.InvalidWaybill)]
    [InlineData("""{"waybill":"RAM 123 456","eventType":"AT_HUB","occurredAt":"2026-10-19T09:00:00Z"}""", RejectReason.InvalidWaybill)]
    [InlineData("""{"waybill":"RAM123456","occurredAt":"2026-10-19T09:00:00Z"}""", RejectReason.MissingEventType)]
    [InlineData("""{"waybill":"RAM123456","eventType":"!!","occurredAt":"2026-10-19T09:00:00Z"}""", RejectReason.InvalidEventType)]
    [InlineData("""{"waybill":"RAM123456","eventType":"9LIVES","occurredAt":"2026-10-19T09:00:00Z"}""", RejectReason.InvalidEventType)]
    [InlineData("""{"waybill":"RAM123456","eventType":"AT_HUB"}""", RejectReason.MissingTimestamp)]
    [InlineData("""{"waybill":"RAM123456","eventType":"AT_HUB","occurredAt":"yesterday"}""", RejectReason.InvalidTimestamp)]
    [InlineData("""{"waybill":"RAM123456","eventType":"AT_HUB","occurredAt":-5}""", RejectReason.InvalidTimestamp)]
    [InlineData("""{"waybill":"RAM123456","eventType":"AT_HUB","occurredAt":"2025-01-01T00:00:00Z"}""", RejectReason.TimestampTooOld)]
    [InlineData("""{"waybill":"RAM123456","eventType":"AT_HUB","occurredAt":"2026-10-19T15:00:00Z"}""", RejectReason.TimestampInFuture)]
    [InlineData("""{"waybill":"RAM123456","eventType":"AT_HUB","occurredAt":"2026-10-19T09:00:00Z","hub":"THIS-HUB-CODE-IS-FAR-TOO-LONG-TO-BE-REAL"}""", RejectReason.FieldTooLong)]
    [InlineData("""{"waybill":"RAM123456","eventType":"AT_HUB",""", RejectReason.MalformedJson)]
    [InlineData("""{"waybill":"RAM123456" "eventType":"AT_HUB"}""", RejectReason.MalformedJson)]
    [InlineData("""[{"waybill":"RAM123456"}]""", RejectReason.NotAnObject)]
    [InlineData("\"just a string\"", RejectReason.NotAnObject)]
    [InlineData("just a string", RejectReason.MalformedJson)]
    [InlineData("", RejectReason.MalformedJson)]
    public void Rejects_invalid_records_with_a_reason(string json, RejectReason expected)
    {
        var result = Parse(json);
        Assert.False(result.IsSuccess);
        Assert.Equal(expected, result.Reason);
    }

    [Fact]
    public void Rejects_oversized_records_before_parsing()
    {
        var parser = new TrackingEventParser(new TransformOptions { MaxRecordBytes = 100 }, HubDirectory.Default, new FixedTimeProvider(Now));
        var result = parser.Parse(new byte[101]);
        Assert.Equal(RejectReason.TooLarge, result.Reason);
    }

    [Fact]
    public void Rejects_invalid_utf8_without_throwing()
    {
        var bytes = Encoding.UTF8.GetBytes("""{"waybill":"RAM123456","eventType":"AT_HUB","occurredAt":"2026-10-19T09:00:00Z","courierId":"X"}""");
        var index = Array.IndexOf(bytes, (byte)'X');
        bytes[index] = 0xFF;
        var result = _parser.Parse(bytes);
        Assert.Equal(RejectReason.MalformedJson, result.Reason);
    }

    [Fact]
    public void Derives_a_stable_id_when_the_producer_omits_it()
    {
        const string json = """{"waybill":"RAM123456","eventType":"AT_HUB","occurredAt":"2026-10-19T09:00:00Z","hub":"JNB01"}""";
        var first = ParseOk(json);
        var second = ParseOk(json.Replace("RAM123456", "ram123456")); // same event after normalisation
        var other = ParseOk(json.Replace("AT_HUB", "DELIVERED"));

        Assert.Equal(first.EventId, second.EventId);
        Assert.NotEqual(first.EventId, other.EventId);
        Assert.Equal(5, (first.EventId.ToByteArray(bigEndian: true)[6] >> 4)); // UUID version 5
    }

    [Fact]
    public void Maps_non_uuid_ids_to_stable_uuids_and_keeps_the_original()
    {
        var e = ParseOk("""{"id":"EVT-000123","waybill":"RAM123456","eventType":"AT_HUB","occurredAt":"2026-10-19T09:00:00Z"}""");
        Assert.Equal(DeterministicGuid.Create(DeterministicGuid.TrackingEventNamespace, "id:EVT-000123"), e.EventId);
        Assert.Contains(new KeyValuePair<string, string>("_source_event_id", "EVT-000123"), e.Attributes);
    }

    [Theory]
    [InlineData("\"latitude\":100,\"longitude\":28")]
    [InlineData("\"latitude\":-26,\"longitude\":200")]
    [InlineData("\"latitude\":0,\"longitude\":0")]
    [InlineData("\"latitude\":-26")]
    [InlineData("\"latitude\":\"abc\",\"longitude\":28")]
    [InlineData("\"latitude\":{\"x\":1},\"longitude\":28")]
    public void Drops_bad_coordinates_but_keeps_the_event(string coordinates)
    {
        var e = ParseOk($$"""{"waybill":"RAM123456","eventType":"AT_HUB","occurredAt":"2026-10-19T09:00:00Z",{{coordinates}}}""");
        Assert.Null(e.Latitude);
        Assert.Null(e.Longitude);
    }

    [Fact]
    public void Keeps_unknown_fields_and_nested_values_as_attributes()
    {
        var e = ParseOk("""
            {"waybill":"RAM123456","eventType":"AT_HUB","occurredAt":"2026-10-19T09:00:00Z",
             "newField":"x","flag":true,"n":1.5,"nested":{"a":[1,2]},"nothing":null,
             "attributes":{"inner":{"b":1},"list":[1,"two"]}}
            """);
        var map = e.Attributes.ToDictionary(kv => kv.Key, kv => kv.Value);
        Assert.Equal("x", map["newField"]);
        Assert.Equal("true", map["flag"]);
        Assert.Equal("1.5", map["n"]);
        Assert.Equal("""{"a":[1,2]}""", map["nested"]);
        Assert.Equal("""{"b":1}""", map["inner"]);
        Assert.Equal("""[1,"two"]""", map["list"]);
        Assert.False(map.ContainsKey("nothing"));
    }

    [Fact]
    public void Bounds_attribute_count_and_value_length()
    {
        var options = new TransformOptions { MaxAttributes = 3, MaxAttributeValueLength = 5 };
        var parser = new TrackingEventParser(options, HubDirectory.Default, new FixedTimeProvider(Now));
        var attrs = string.Join(",", Enumerable.Range(0, 10).Select(i => $"\"k{i}\":\"value-{i}\""));
        var json = """{"waybill":"RAM123456","eventType":"AT_HUB","occurredAt":"2026-10-19T09:00:00Z","attributes":{""" + attrs + "}}";
        var result = parser.Parse(Encoding.UTF8.GetBytes(json));
        Assert.True(result.IsSuccess);
        Assert.Equal(3, result.Event!.Attributes.Count);
        Assert.All(result.Event.Attributes, kv => Assert.True(kv.Value.Length <= 5));
    }

    [Fact]
    public void Tolerates_comments_trailing_commas_and_duplicate_keys()
    {
        var e = ParseOk("""
            {
              // scanner firmware 4.2 adds comments
              "waybill": "RAM000000001",
              "eventType": "COLLECTED",
              "eventType": "AT_HUB",
              "occurredAt": "2026-10-19T09:00:00Z",
            }
            """);
        Assert.Equal(EventTypes.AtHub, e.EventType);
    }

    [Fact]
    public void Unknown_hub_gets_unknown_region()
    {
        var e = ParseOk("""{"waybill":"RAM123456","eventType":"AT_HUB","occurredAt":"2026-10-19T09:00:00Z","hub":"zzz9"}""");
        Assert.Equal("ZZZ9", e.HubCode);
        Assert.Equal(HubDirectory.UnknownRegion, e.Region);
    }

    [Fact]
    public void Never_throws_on_random_bytes()
    {
        var random = new Random(7);
        var seeds = new[]
        {
            Encoding.UTF8.GetBytes("""{"waybill":"RAM123456","eventType":"AT_HUB","occurredAt":"2026-10-19T09:00:00Z","attributes":{"a":[1,{"b":null}]}}"""),
        };
        for (var i = 0; i < 20_000; i++)
        {
            byte[] input;
            if (i % 2 == 0)
            {
                input = new byte[random.Next(0, 200)];
                random.NextBytes(input);
            }
            else
            {
                // Mutate a valid record: flip, drop or duplicate bytes.
                input = (byte[])seeds[0].Clone();
                for (var m = 0; m < 1 + random.Next(4); m++)
                {
                    input[random.Next(input.Length)] = (byte)random.Next(256);
                }
            }

            var result = _parser.Parse(input);
            Assert.True(result.IsSuccess || result.Reason != RejectReason.None);
        }
    }

    [Fact]
    public void Generated_load_is_classified_exactly_as_the_generator_intends()
    {
        var generator = new EventingC3.LoadGen.TrackingEventGenerator(1, 0.05, 0.0, 0.05, now: Now);
        var parser = new TrackingEventParser(new TransformOptions(), HubDirectory.Default, new FixedTimeProvider(Now));
        for (var i = 0; i < 5_000; i++)
        {
            var payload = generator.Next(out var kind, out _);
            var result = parser.Parse(payload);
            Assert.Equal(kind == EventingC3.LoadGen.GeneratedKind.Invalid, !result.IsSuccess);
        }
    }
}
