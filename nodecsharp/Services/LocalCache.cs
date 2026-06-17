using System.Collections.Concurrent;
using CacheNode.Models;

namespace CacheNode.Services;

public sealed class LocalCache
{
    private readonly ConcurrentDictionary<string, CacheEntry> _storage = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, object> _locks = new(StringComparer.Ordinal);
    private readonly ILogger<LocalCache> _logger;

    public LocalCache(ILogger<LocalCache> logger)
    {
        _logger = logger;
    }

    public string? Get(string key)
    {
        return _storage.TryGetValue(key, out var entry) ? entry.Value : null;
    }

    public bool Put(string key, string value, long timestamp)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            return false;
        }

        if (timestamp <= 0)
        {
            timestamp = TimeProvider.UnixMilliseconds();
        }

        var gate = _locks.GetOrAdd(key, _ => new object());

        lock (gate)
        {
            if (_storage.TryGetValue(key, out var current) && current.Timestamp > timestamp)
            {
                _logger.LogInformation(
                    "Ignored stale cache update for {Key}. Current timestamp={Current}, incoming timestamp={Incoming}",
                    key,
                    current.Timestamp,
                    timestamp);
                return false;
            }

            _storage[key] = new CacheEntry(value, timestamp);
            _logger.LogInformation("Local cache updated: {Key} = {Value}", key, value);
            return true;
        }
    }

    public IReadOnlyDictionary<string, string> Snapshot()
    {
        return _storage.ToDictionary(item => item.Key, item => item.Value.Value, StringComparer.Ordinal);
    }

    public void ReplaceFromSnapshot(IReadOnlyDictionary<string, string> snapshot)
    {
        var timestamp = TimeProvider.UnixMilliseconds();
        foreach (var (key, value) in snapshot)
        {
            Put(key, value, timestamp);
        }
    }
}
