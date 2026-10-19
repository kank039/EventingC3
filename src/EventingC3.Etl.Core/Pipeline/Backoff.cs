namespace EventingC3.Etl.Core.Pipeline;

/// <summary>Exponential back-off with full jitter, so a fleet of workers does not retry in lock-step against a recovering cluster.</summary>
public static class Backoff
{
    public static TimeSpan Delay(int attempt, TimeSpan initial, TimeSpan max)
    {
        var exponent = Math.Min(attempt, 20);
        var ceiling = Math.Min(max.TotalMilliseconds, initial.TotalMilliseconds * Math.Pow(2, exponent));
        var jittered = ceiling / 2 + Random.Shared.NextDouble() * ceiling / 2;
        return TimeSpan.FromMilliseconds(jittered);
    }
}
