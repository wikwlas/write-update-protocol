namespace CacheNode.Models;

public sealed class NodeOptions
{
    public int Id { get; set; } = 1;
    public string Ip { get; set; } = "0.0.0.0";
    public int HttpPort { get; set; } = 8081;
    public int UdpPort { get; set; } = 4444;
    public int InitialLeaderId { get; set; } = 3;
    public int HeartbeatIntervalMs { get; set; } = 2000;
    public int HeartbeatTimeoutMs { get; set; } = 5000;
    public int RequestTimeoutMs { get; set; } = 1500;
    public Dictionary<int, PeerInfo> Peers { get; set; } = new();
}
