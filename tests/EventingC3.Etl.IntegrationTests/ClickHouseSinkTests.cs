using System.Text;
using EventingC3.Etl.Core.Abstractions;
using EventingC3.Etl.Core.Domain;
using EventingC3.Etl.Core.Serialization;
using EventingC3.Etl.Core.Transform;
using EventingC3.Etl.Infrastructure.ClickHouse;
using EventingC3.Etl.TestSupport;
using Microsoft.Extensions.Logging.Abstractions;

namespace EventingC3.Etl.IntegrationTests;

/// <summary>Validates the RowBinary encoding, schema and error handling against a real ClickHouse server.</summary>
public class ClickHouseSinkTests
{
    private static byte[] Encode(IEnumerable<TrackingEvent> events, long ingestedAt = 1_760_000_000_000)
    {
        using var buffer = new PooledBuffer(1024);
        var writer = new RowBinaryWriter(buffer);
        foreach (var e in events)
        {
            TrackingEventEncoder.Write(writer, e, ingestedAt);
        }

        return buffer.WrittenSpan.ToArray();
    }

    private static TrackingEvent Event(string waybill, Guid? id = null, long? occurred = null) => new()
    {
        EventId = id ?? Guid.NewGuid(),
        Waybill = waybill,
        EventType = EventTypes.AtHub,
        OccurredAtUnixMs = occurred ?? DateTimeOffset.UtcNow.AddHours(-1).ToUnixTimeMilliseconds(),
        HubCode = "JNB01",
        Region = "GAUTENG",
    };

    [IntegrationFact]
    public async Task Schema_is_idempotent()
    {
        await using var warehouse = await TestWarehouse.CreateAsync();
        await warehouse.Sink.ApplySchemaAsync(CancellationToken.None); // second application must be a no-op
        Assert.Equal(5, await warehouse.ScalarAsync($"SELECT count() FROM system.tables WHERE database = '{warehouse.Options.Database}'"));
    }

    [IntegrationFact]
    public async Task Every_column_round_trips_through_clickhouse()
    {
        await using var warehouse = await TestWarehouse.CreateAsync();
        var id = Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e");
        var occurred = new DateTimeOffset(2026, 10, 19, 8, 15, 0, 250, TimeSpan.Zero).ToUnixTimeMilliseconds();
        var full = new TrackingEvent
        {
            EventId = id, Waybill = "RAM000000123", EventType = "DELIVERED", OccurredAtUnixMs = occurred,
            HubCode = "JNB01", Region = "GAUTENG", CourierId = "C123", Latitude = -26.2041, Longitude = 28.0473,
            SourceSystem = "scanner-app", SchemaVersion = 2,
            Attributes = [new("signedBy", "Thandi Nkosi ✓"), new("pieces", "3")], SourcePartition = 7, SourceOffset = 123456789012,
        };
        var sparse = new TrackingEvent { EventId = Guid.Empty, Waybill = "RAM9", EventType = "AT_HUB", OccurredAtUnixMs = occurred };

        await warehouse.Sink.InsertAsync(Encode([full, sparse]), 2, "t1", CancellationToken.None);

        var rows = await warehouse.RowsAsync(
            "SELECT toString(event_id) AS id, waybill, event_type, toUnixTimestamp64Milli(occurred_at) AS occurred, hub_code, region, courier_id, " +
            "latitude, longitude, source_system, schema_version, attributes, toUnixTimestamp64Milli(ingested_at) AS ingested, source_partition, " +
            "toString(source_offset) AS source_offset FROM tracking_events ORDER BY waybill");

        Assert.Equal(2, rows.Count);
        var r = rows[0];
        Assert.Equal(id.ToString(), r.GetProperty("id").GetString());
        Assert.Equal("RAM000000123", r.GetProperty("waybill").GetString());
        Assert.Equal("DELIVERED", r.GetProperty("event_type").GetString());
        Assert.Equal(occurred.ToString(), r.GetProperty("occurred").ToString());
        Assert.Equal("JNB01", r.GetProperty("hub_code").GetString());
        Assert.Equal("GAUTENG", r.GetProperty("region").GetString());
        Assert.Equal("C123", r.GetProperty("courier_id").GetString());
        Assert.Equal(-26.2041, r.GetProperty("latitude").GetDouble());
        Assert.Equal(28.0473, r.GetProperty("longitude").GetDouble());
        Assert.Equal("scanner-app", r.GetProperty("source_system").GetString());
        Assert.Equal(2, r.GetProperty("schema_version").GetInt32());
        Assert.Equal("Thandi Nkosi ✓", r.GetProperty("attributes").GetProperty("signedBy").GetString());
        Assert.Equal("3", r.GetProperty("attributes").GetProperty("pieces").GetString());
        Assert.Equal("1760000000000", r.GetProperty("ingested").ToString());
        Assert.Equal(7, r.GetProperty("source_partition").GetInt32());
        Assert.Equal("123456789012", r.GetProperty("source_offset").GetString());

        var s = rows[1];
        Assert.Equal(Guid.Empty.ToString(), s.GetProperty("id").GetString());
        Assert.Equal(System.Text.Json.JsonValueKind.Null, s.GetProperty("latitude").ValueKind);
        Assert.Equal(string.Empty, s.GetProperty("courier_id").GetString());
    }

    [IntegrationFact]
    public async Task Retried_insert_with_the_same_token_is_written_once()
    {
        await using var warehouse = await TestWarehouse.CreateAsync();
        var rows = Encode(Enumerable.Range(0, 1000).Select(i => Event($"RAM{i:D9}")));

        await warehouse.Sink.InsertAsync(rows, 1000, "batch-token-1", CancellationToken.None);
        await warehouse.Sink.InsertAsync(rows, 1000, "batch-token-1", CancellationToken.None); // e.g. retry after a timeout

        Assert.Equal(1000, await warehouse.ScalarAsync("SELECT count() FROM tracking_events"));
        Assert.Equal(1000, await warehouse.ScalarAsync("SELECT sum(events) FROM tracking_events_hourly")); // MVs deduplicated too
    }

    [IntegrationFact]
    public async Task Replayed_rows_with_a_new_token_collapse_under_final()
    {
        await using var warehouse = await TestWarehouse.CreateAsync();
        var rows = Encode(Enumerable.Range(0, 500).Select(i => Event($"RAM{i:D9}", new Guid(i, 0, 0, new byte[8]), 1_760_000_000_000 + i)));

        await warehouse.Sink.InsertAsync(rows, 500, "a", CancellationToken.None);
        await warehouse.Sink.InsertAsync(rows, 500, "b", CancellationToken.None); // replay with different batch boundaries

        Assert.Equal(1000, await warehouse.ScalarAsync("SELECT count() FROM tracking_events"));
        Assert.Equal(500, await warehouse.ScalarAsync("SELECT count() FROM tracking_events FINAL"));
        await warehouse.Sink.ExecuteAsync("OPTIMIZE TABLE tracking_events FINAL", warehouse.Options.Database);
        Assert.Equal(500, await warehouse.ScalarAsync("SELECT count() FROM tracking_events"));
    }

    [IntegrationFact]
    public async Task Latest_status_view_tracks_the_newest_event_per_waybill()
    {
        await using var warehouse = await TestWarehouse.CreateAsync();
        var t0 = DateTimeOffset.UtcNow.AddHours(-3).ToUnixTimeMilliseconds();
        var events = new[]
        {
            Event("RAM000000001", occurred: t0) with { EventType = "COLLECTED" },
            Event("RAM000000001", occurred: t0 + 7_200_000) with { EventType = "DELIVERED" },
            Event("RAM000000001", occurred: t0 + 3_600_000) with { EventType = "OUT_FOR_DELIVERY" }, // late arrival
        };
        await warehouse.Sink.InsertAsync(Encode(events), 3, "s", CancellationToken.None);

        var status = await warehouse.RowsAsync("SELECT event_type FROM consignment_latest_status FINAL WHERE waybill = 'RAM000000001'");
        Assert.Equal("DELIVERED", Assert.Single(status).GetProperty("event_type").GetString());
    }

    [IntegrationFact]
    public async Task Corrupt_data_is_classified_as_invalid_data()
    {
        await using var warehouse = await TestWarehouse.CreateAsync();
        var good = Encode([Event("RAM000000001")]);
        var corrupt = good[..^5]; // truncated row

        var ex = await Assert.ThrowsAsync<SinkException>(() => warehouse.Sink.InsertAsync(corrupt, 1, "c", CancellationToken.None));
        Assert.Equal(SinkErrorKind.InvalidData, ex.Kind);
    }

    [IntegrationFact]
    public async Task Missing_table_is_classified_as_fatal()
    {
        await using var warehouse = await TestWarehouse.CreateAsync();
        using var wrong = new ClickHouseSink(new ClickHouseOptions { Endpoint = Env.ClickHouse, Database = warehouse.Options.Database, Table = "nope" },
            NullLogger<ClickHouseSink>.Instance);
        var ex = await Assert.ThrowsAsync<SinkException>(() => wrong.InsertAsync(Encode([Event("RAM000000001")]), 1, "f", CancellationToken.None));
        Assert.Equal(SinkErrorKind.Fatal, ex.Kind);
    }

    [IntegrationFact]
    public async Task Unreachable_server_is_classified_as_transient()
    {
        using var down = new ClickHouseSink(new ClickHouseOptions { Endpoint = "http://127.0.0.1:1", RequestTimeout = TimeSpan.FromSeconds(5) },
            NullLogger<ClickHouseSink>.Instance);
        var ex = await Assert.ThrowsAsync<SinkException>(() => down.InsertAsync(new byte[] { 1 }, 1, "x", CancellationToken.None));
        Assert.Equal(SinkErrorKind.Transient, ex.Kind);
    }

    [IntegrationFact]
    public async Task Gzip_and_brotli_compressed_inserts_are_accepted()
    {
        await using var warehouse = await TestWarehouse.CreateAsync();
        foreach (var compression in new[] { "gzip", "br" })
        {
            using var sink = new ClickHouseSink(new ClickHouseOptions
            {
                Endpoint = Env.ClickHouse, Database = warehouse.Options.Database, Compression = compression,
            }, NullLogger<ClickHouseSink>.Instance);
            await sink.InsertAsync(Encode(Enumerable.Range(0, 100).Select(i => Event($"RAM{i:D9}"))), 100, compression, CancellationToken.None);
        }

        Assert.Equal(200, await warehouse.ScalarAsync("SELECT count() FROM tracking_events"));
    }

    [IntegrationFact]
    public async Task Parser_output_for_generated_load_inserts_cleanly()
    {
        await using var warehouse = await TestWarehouse.CreateAsync();
        var generator = new LoadGen.TrackingEventGenerator(9, 0, 0, 0.1);
        var parser = new TrackingEventParser(new TransformOptions(), HubDirectory.Default, TimeProvider.System);
        var events = Enumerable.Range(0, 10_000).Select(i => parser.Parse(generator.Next(out _, out _)).Event!).ToList();

        await warehouse.Sink.InsertAsync(Encode(events), events.Count, "gen", CancellationToken.None);
        Assert.Equal(10_000, await warehouse.ScalarAsync("SELECT uniqExact(event_id) FROM tracking_events"));
        Assert.Equal(0, await warehouse.ScalarAsync("SELECT countIf(waybill != upper(waybill)) FROM tracking_events"));
    }
}
