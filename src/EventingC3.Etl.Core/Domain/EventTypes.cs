using System.Collections.Frozen;

namespace EventingC3.Etl.Core.Domain;

/// <summary>
/// Canonicalises event type names. Producers disagree on spelling ("POD", "delivered", "Delivered-OK"),
/// so aliases collapse to one canonical value; unknown but well-formed types are kept rather than rejected,
/// so a new producer event type never causes data loss.
/// </summary>
public static class EventTypes
{
    public const string Collected = "COLLECTED";
    public const string AtHub = "AT_HUB";
    public const string InTransit = "IN_TRANSIT";
    public const string OutForDelivery = "OUT_FOR_DELIVERY";
    public const string Delivered = "DELIVERED";
    public const string DeliveryFailed = "DELIVERY_FAILED";
    public const string Returned = "RETURNED";
    public const string Created = "CREATED";

    /// <summary>Longest canonical event type accepted.</summary>
    public const int MaxLength = 48;

    private static readonly FrozenDictionary<string, string> Aliases = new Dictionary<string, string>
    {
        ["POD"] = Delivered,
        ["DELIVERY"] = Delivered,
        ["DELIVERED_OK"] = Delivered,
        ["PICKUP"] = Collected,
        ["PICKED_UP"] = Collected,
        ["COLLECTION"] = Collected,
        ["HUB_ARRIVAL"] = AtHub,
        ["ARRIVED_AT_HUB"] = AtHub,
        ["HUB_SCAN"] = AtHub,
        ["LINEHAUL"] = InTransit,
        ["DEPARTED_HUB"] = InTransit,
        ["OFD"] = OutForDelivery,
        ["ON_ROUTE"] = OutForDelivery,
        ["FAILED_DELIVERY"] = DeliveryFailed,
        ["EXCEPTION"] = DeliveryFailed,
        ["RTS"] = Returned,
        ["RETURN_TO_SENDER"] = Returned,
        ["BOOKED"] = Created,
        ["MANIFESTED"] = Created,
    }.ToFrozenDictionary(StringComparer.Ordinal);

    /// <summary>
    /// Normalises <paramref name="raw"/> to upper snake case and resolves aliases.
    /// Returns <c>false</c> when the value cannot be a valid event type.
    /// </summary>
    public static bool TryNormalize(ReadOnlySpan<char> raw, out string canonical)
    {
        canonical = string.Empty;
        raw = raw.Trim();
        if (raw.IsEmpty || raw.Length > MaxLength)
        {
            return false;
        }

        Span<char> buffer = stackalloc char[raw.Length];
        for (var i = 0; i < raw.Length; i++)
        {
            var c = raw[i];
            if (c is ' ' or '-' or '.')
            {
                c = '_';
            }
            else if (char.IsAsciiLetterLower(c))
            {
                c = (char)(c - 32);
            }
            else if (!char.IsAsciiLetterUpper(c) && !char.IsAsciiDigit(c) && c != '_')
            {
                return false;
            }

            buffer[i] = c;
        }

        if (!char.IsAsciiLetterUpper(buffer[0]))
        {
            return false;
        }

        var lookup = Aliases.GetAlternateLookup<ReadOnlySpan<char>>();
        if (lookup.TryGetValue(buffer, out var alias))
        {
            canonical = alias;
            return true;
        }

        canonical = Intern(buffer);
        return true;
    }

    private static readonly FrozenDictionary<string, string> Canonical = new[]
    {
        Collected, AtHub, InTransit, OutForDelivery, Delivered, DeliveryFailed, Returned, Created,
    }.ToFrozenDictionary(v => v, v => v, StringComparer.Ordinal);

    // Avoids allocating a new string for the overwhelmingly common canonical values.
    private static string Intern(ReadOnlySpan<char> value) =>
        Canonical.GetAlternateLookup<ReadOnlySpan<char>>().TryGetValue(value, out var known) ? known : value.ToString();
}
