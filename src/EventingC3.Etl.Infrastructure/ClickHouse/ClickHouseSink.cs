using System.IO.Compression;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using EventingC3.Etl.Core.Abstractions;
using EventingC3.Etl.Core.Serialization;
using Microsoft.Extensions.Logging;

namespace EventingC3.Etl.Infrastructure.ClickHouse;

/// <summary>
/// Loads RowBinary blocks through the ClickHouse HTTP interface. One HTTP request = one atomic insert
/// (batches stay under <c>max_insert_block_size</c>), tagged with an <c>insert_deduplication_token</c>.
/// </summary>
public sealed class ClickHouseSink : IEventSink, IDisposable
{
    private const int MaxErrorBodyChars = 4096;

    private readonly ClickHouseOptions _options;
    private readonly ILogger<ClickHouseSink> _logger;
    private readonly HttpClient _http;
    private readonly string _insertUriPrefix;

    public ClickHouseSink(ClickHouseOptions options, ILogger<ClickHouseSink> logger)
    {
        _options = options;
        _logger = logger;
        _http = CreateHttpClient(options);

        var query = $"INSERT INTO `{options.Database}`.`{options.Table}` ({string.Join(", ", TrackingEventEncoder.Columns)}) FORMAT RowBinary";
        var settings = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["query"] = query,
            ["insert_deduplicate"] = "1",
            ["deduplicate_blocks_in_dependent_materialized_views"] = "1",
            ["async_insert"] = "0",
            // Keep each request a single block so the insert is atomic and the token covers all of it.
            ["max_insert_block_size"] = "1048576",
            ["min_insert_block_size_rows"] = "0",
            ["min_insert_block_size_bytes"] = "0",
            // Buffer the response so failures are always reported through the HTTP status code.
            ["wait_end_of_query"] = "1",
        };
        foreach (var (key, value) in options.Settings)
        {
            settings[key] = value;
        }

        var sb = new StringBuilder(options.Endpoint.TrimEnd('/')).Append("/?");
        foreach (var (key, value) in settings)
        {
            sb.Append(Uri.EscapeDataString(key)).Append('=').Append(Uri.EscapeDataString(value)).Append('&');
        }

        _insertUriPrefix = sb.Append("insert_deduplication_token=").ToString();
    }

    internal static HttpClient CreateHttpClient(ClickHouseOptions options)
    {
        var handler = new SocketsHttpHandler
        {
            PooledConnectionLifetime = TimeSpan.FromMinutes(5), // pick up DNS / load-balancer changes
            PooledConnectionIdleTimeout = TimeSpan.FromSeconds(50),
            MaxConnectionsPerServer = options.MaxConnections,
            ConnectTimeout = TimeSpan.FromSeconds(10),
            AutomaticDecompression = DecompressionMethods.None,
        };
        var http = new HttpClient(handler) { Timeout = Timeout.InfiniteTimeSpan };
        http.DefaultRequestHeaders.Add("X-ClickHouse-User", options.Username);
        if (!string.IsNullOrEmpty(options.Password))
        {
            http.DefaultRequestHeaders.Add("X-ClickHouse-Key", options.Password);
        }

        return http;
    }

    public async Task InsertAsync(ReadOnlyMemory<byte> rows, int rowCount, string deduplicationToken, CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(_options.RequestTimeout);

        using var request = new HttpRequestMessage(HttpMethod.Post, _insertUriPrefix + Uri.EscapeDataString(deduplicationToken))
        {
            Content = CreateContent(rows),
        };

        HttpResponseMessage response;
        try
        {
            response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, timeout.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException ex) when (!cancellationToken.IsCancellationRequested)
        {
            throw new SinkException(SinkErrorKind.Transient, $"Insert timed out after {_options.RequestTimeout}", ex);
        }
        catch (HttpRequestException ex)
        {
            throw new SinkException(SinkErrorKind.Transient, $"ClickHouse unreachable: {ex.Message}", ex);
        }

        using (response)
        {
            if (response.IsSuccessStatusCode)
            {
                return;
            }

            string body;
            try
            {
                body = await response.Content.ReadAsStringAsync(timeout.Token).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is HttpRequestException or IOException or OperationCanceledException && !cancellationToken.IsCancellationRequested)
            {
                body = string.Empty;
            }

            if (body.Length > MaxErrorBodyChars)
            {
                body = body[..MaxErrorBodyChars];
            }

            var headerCode = response.Headers.TryGetValues("X-ClickHouse-Exception-Code", out var values) ? values.FirstOrDefault() : null;
            var code = ClickHouseErrorClassifier.ExtractCode(headerCode, body);
            var kind = ClickHouseErrorClassifier.Classify(response.StatusCode, code);
            throw new SinkException(kind, $"ClickHouse HTTP {(int)response.StatusCode} code {code?.ToString() ?? "n/a"}: {body.Trim()}");
        }
    }

    private HttpContent CreateContent(ReadOnlyMemory<byte> rows)
    {
        HttpContent content;
        switch (_options.Compression.ToLowerInvariant())
        {
            case "gzip":
                content = new ByteArrayContent(Compress(rows, s => new GZipStream(s, CompressionLevel.Fastest, leaveOpen: true)));
                content.Headers.ContentEncoding.Add("gzip");
                break;
            case "br":
                content = new ByteArrayContent(Compress(rows, s => new BrotliStream(s, CompressionLevel.Fastest, leaveOpen: true)));
                content.Headers.ContentEncoding.Add("br");
                break;
            default:
                content = new ReadOnlyMemoryContent(rows);
                break;
        }

        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        return content;
    }

    private static byte[] Compress(ReadOnlyMemory<byte> rows, Func<Stream, Stream> wrap)
    {
        using var output = new MemoryStream(rows.Length / 4);
        using (var compressor = wrap(output))
        {
            compressor.Write(rows.Span);
        }

        return output.ToArray();
    }

    /// <summary>Runs a non-insert statement (DDL, queries in tests). Returns the response body.</summary>
    public async Task<string> ExecuteAsync(string sql, string? database = null, CancellationToken cancellationToken = default)
    {
        var uri = _options.Endpoint.TrimEnd('/') + "/?wait_end_of_query=1" +
                  (database is null ? string.Empty : "&database=" + Uri.EscapeDataString(database));
        using var content = new StringContent(sql, Encoding.UTF8, "text/plain");
        using var response = await _http.PostAsync(uri, content, cancellationToken).ConfigureAwait(false);
        var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException($"ClickHouse HTTP {(int)response.StatusCode}: {body}");
        }

        return body;
    }

    /// <summary>Creates the database and applies the embedded schema. Every statement is idempotent.</summary>
    public async Task ApplySchemaAsync(CancellationToken cancellationToken)
    {
        await ExecuteAsync($"CREATE DATABASE IF NOT EXISTS `{_options.Database}`", cancellationToken: cancellationToken).ConfigureAwait(false);
        foreach (var statement in ClickHouseSchema.Statements())
        {
            await ExecuteAsync(statement, _options.Database, cancellationToken).ConfigureAwait(false);
        }

        _logger.LogInformation("ClickHouse schema applied to database {Database}", _options.Database);
    }

    public void Dispose() => _http.Dispose();
}
