using System.Collections.Concurrent;

namespace CacheNode.Services;

public sealed class DirectoryManager
{
    private readonly object _directoryLock = new();
    private readonly ConcurrentDictionary<string, HashSet<int>> _presenceList = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, string> _mainMemory = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _variableWriteQueues = new(StringComparer.Ordinal);
    private readonly ILogger<DirectoryManager> _logger;

    public DirectoryManager(ILogger<DirectoryManager> logger)
    {
        _logger = logger;
    }

    public async Task<T> WithVariableWriteLockAsync<T>(string variableName, Func<Task<T>> work)
    {
        var queue = _variableWriteQueues.GetOrAdd(variableName, _ => new SemaphoreSlim(1, 1));
        await queue.WaitAsync();
        try
        {
            return await work();
        }
        finally
        {
            queue.Release();
        }
    }

    public void RegisterVariablePresence(string variableName, int nodeId)
    {
        lock (_directoryLock)
        {
            var owners = _presenceList.GetOrAdd(variableName, _ => new HashSet<int>());
            owners.Add(nodeId);
        }
    }

    public void UpdateMainMemoryValue(string variableName, string value)
    {
        _mainMemory[variableName] = value;
    }

    public IReadOnlySet<int> GetOwnersOf(string variableName)
    {
        lock (_directoryLock)
        {
            return _presenceList.TryGetValue(variableName, out var owners)
                ? owners.ToHashSet()
                : new HashSet<int>();
        }
    }

    public void Clear()
    {
        lock (_directoryLock)
        {
            _presenceList.Clear();
            _mainMemory.Clear();
        }

        _logger.LogInformation("DirectoryManager state cleared before reconstruction.");
    }

    public void RemoveNodeFromPresence(int nodeId)
    {
        lock (_directoryLock)
        {
            foreach (var (_, owners) in _presenceList)
            {
                owners.Remove(nodeId);
            }
        }
    }

    public object Snapshot()
    {
        lock (_directoryLock)
        {
            var presence = _presenceList.ToDictionary(
                item => item.Key,
                item => item.Value.OrderBy(id => id).ToArray(),
                StringComparer.Ordinal);

            var memory = _mainMemory.ToDictionary(item => item.Key, item => item.Value, StringComparer.Ordinal);

            return new
            {
                presenceList = presence,
                mainMemory = memory
            };
        }
    }
}
