using System.Collections.Frozen;

namespace EventingC3.Etl.Core.Domain;

/// <summary>
/// Reference data used to enrich events with a region. Lookups are lock-free (frozen dictionary)
/// because they run once per record on the hot path.
/// </summary>
public sealed class HubDirectory
{
    /// <summary>Region assigned when a hub code is missing or not in the directory.</summary>
    public const string UnknownRegion = "UNKNOWN";

    private readonly FrozenDictionary<string, string> _regions;

    /// <summary>Creates a directory from hub-code → region pairs (codes are matched case-insensitively).</summary>
    public HubDirectory(IEnumerable<KeyValuePair<string, string>> hubRegions)
    {
        _regions = hubRegions.ToFrozenDictionary(
            kv => kv.Key.Trim().ToUpperInvariant(),
            kv => kv.Value.Trim().ToUpperInvariant(),
            StringComparer.Ordinal);
    }

    /// <summary>A directory seeded with RAM's main hubs; production deployments override this from configuration.</summary>
    public static HubDirectory Default { get; } = new(new Dictionary<string, string>
    {
        ["JNB01"] = "GAUTENG",
        ["JNB02"] = "GAUTENG",
        ["PTA01"] = "GAUTENG",
        ["CPT01"] = "WESTERN_CAPE",
        ["DUR01"] = "KWAZULU_NATAL",
        ["PMB01"] = "KWAZULU_NATAL",
        ["PLZ01"] = "EASTERN_CAPE",
        ["ELS01"] = "EASTERN_CAPE",
        ["BFN01"] = "FREE_STATE",
        ["KIM01"] = "NORTHERN_CAPE",
        ["NLP01"] = "MPUMALANGA",
        ["PLK01"] = "LIMPOPO",
        ["RTB01"] = "NORTH_WEST",
    });

    /// <summary>Returns the region for an already upper-cased hub code.</summary>
    public string RegionFor(string hubCode) =>
        hubCode.Length != 0 && _regions.TryGetValue(hubCode, out var region) ? region : UnknownRegion;
}
