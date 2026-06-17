namespace CacheNode.Models;

public sealed class UserUpdateRequest
{
    public string Key { get; set; } = string.Empty;
    public string Value { get; set; } = string.Empty;
}
