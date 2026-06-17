namespace CacheNode.Models;

public sealed class CacheUpdateRequest
{
    public int SenderNodeId { get; set; }
    public string VariableName { get; set; } = string.Empty;
    public string NewValue { get; set; } = string.Empty;
    public long Timestamp { get; set; }
}
