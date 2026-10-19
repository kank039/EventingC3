namespace EventingC3.Etl.Core.Observability;

/// <summary>Readiness signal: the worker is not ready while its sink keeps failing or after the pipeline has faulted.</summary>
public sealed class EtlHealth
{
    private int _consecutiveFailures;
    private volatile string? _lastError;
    private volatile string? _fault;

    public int ConsecutiveFailures => Volatile.Read(ref _consecutiveFailures);

    public string? LastError => _lastError;

    public string? Fault => _fault;

    public void Succeeded()
    {
        Volatile.Write(ref _consecutiveFailures, 0);
        _lastError = null;
    }

    public void Failed(string error)
    {
        Interlocked.Increment(ref _consecutiveFailures);
        _lastError = error;
    }

    public void Faulted(string error) => _fault = error;

    public bool IsReady(int unhealthyAfter) => _fault is null && ConsecutiveFailures < unhealthyAfter;
}
