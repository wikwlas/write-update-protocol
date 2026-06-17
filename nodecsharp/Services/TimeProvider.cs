namespace CacheNode.Services;

public static class TimeProvider
{
    public static long UnixMilliseconds() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}
