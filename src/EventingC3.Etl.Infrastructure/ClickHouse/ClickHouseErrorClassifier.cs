using System.Collections.Frozen;
using System.Net;
using System.Text.RegularExpressions;
using EventingC3.Etl.Core.Abstractions;

namespace EventingC3.Etl.Infrastructure.ClickHouse;

/// <summary>
/// Maps ClickHouse error codes to pipeline reactions. Codes come from
/// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ErrorCodes.cpp.
/// </summary>
public static partial class ClickHouseErrorClassifier
{
    /// <summary>The server rejected the data itself — bisect the batch to find the rows.</summary>
    private static readonly FrozenSet<int> InvalidDataCodes = new[]
    {
        6,   // CANNOT_PARSE_TEXT
        26,  // CANNOT_PARSE_QUOTED_STRING
        27,  // CANNOT_PARSE_INPUT_ASSERTION_FAILED
        32,  // ATTEMPT_TO_READ_AFTER_EOF
        33,  // CANNOT_READ_ALL_DATA
        38,  // CANNOT_PARSE_DATE
        41,  // CANNOT_PARSE_DATETIME
        53,  // TYPE_MISMATCH
        69,  // ARGUMENT_OUT_OF_BOUND
        70,  // CANNOT_CONVERT_TYPE
        72,  // CANNOT_PARSE_NUMBER
        117, // INCORRECT_DATA
        131, // TOO_LARGE_STRING_SIZE
        349, // CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN
        395, // FUNCTION_THROW_IF_VALUE_IS_NON_ZERO
        469, // VIOLATED_CONSTRAINT
    }.ToFrozenSet();

    /// <summary>Configuration, schema or permission errors: retrying the same request cannot succeed until an operator acts.</summary>
    private static readonly FrozenSet<int> FatalCodes = new[]
    {
        16,  // NO_SUCH_COLUMN_IN_TABLE
        47,  // UNKNOWN_IDENTIFIER
        60,  // UNKNOWN_TABLE
        62,  // SYNTAX_ERROR
        81,  // UNKNOWN_DATABASE
        115, // UNKNOWN_SETTING
        192, // UNKNOWN_USER
        193, // WRONG_PASSWORD
        194, // REQUIRED_PASSWORD
        497, // ACCESS_DENIED
        516, // AUTHENTICATION_FAILED
    }.ToFrozenSet();

    [GeneratedRegex(@"Code:\s*(\d+)", RegexOptions.CultureInvariant)]
    private static partial Regex CodePattern();

    /// <summary>Extracts the ClickHouse error code from the response header or body; null when absent.</summary>
    public static int? ExtractCode(string? headerValue, string body)
    {
        if (int.TryParse(headerValue, out var fromHeader))
        {
            return fromHeader;
        }

        var match = CodePattern().Match(body);
        return match.Success && int.TryParse(match.Groups[1].ValueSpan, out var fromBody) ? fromBody : null;
    }

    public static SinkErrorKind Classify(HttpStatusCode status, int? code)
    {
        if (code is { } c)
        {
            if (InvalidDataCodes.Contains(c))
            {
                return SinkErrorKind.InvalidData;
            }

            if (FatalCodes.Contains(c))
            {
                return SinkErrorKind.Fatal;
            }

            // Everything else with a code (TOO_MANY_PARTS 252, MEMORY_LIMIT_EXCEEDED 241, TIMEOUT_EXCEEDED 159,
            // TOO_MANY_SIMULTANEOUS_QUERIES 202, UNKNOWN_STATUS_OF_INSERT 319, KEEPER_EXCEPTION 999, ...) is
            // treated as transient: retried with the same deduplication token.
            return SinkErrorKind.Transient;
        }

        return status switch
        {
            HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden or HttpStatusCode.NotFound => SinkErrorKind.Fatal,
            _ => SinkErrorKind.Transient,
        };
    }
}
