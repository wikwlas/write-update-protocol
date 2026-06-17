namespace CacheNode.Models;

public sealed class ElectionMessage
{
    public int SenderNodeId { get; set; }
    public string Type { get; set; } = string.Empty;
}
