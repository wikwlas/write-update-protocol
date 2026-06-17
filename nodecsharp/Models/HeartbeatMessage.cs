namespace CacheNode.Models;

public sealed class HeartbeatMessage
{
    public int NodeId { get; set; }
    public string Status { get; set; } = "ALIVE";
    public long Timestamp { get; set; }
}
