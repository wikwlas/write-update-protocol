namespace CacheNode.Models;

public sealed record ReplicationResult(int PeerId, bool Success, string? ErrorMessage = null);
