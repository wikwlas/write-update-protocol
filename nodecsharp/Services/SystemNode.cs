using CacheNode.Models;

namespace CacheNode.Services;

public sealed class SystemNode
{
    private readonly object _stateLock = new();
    private int _leaderId;
    private string _state = "NORMAL";

    public SystemNode(NodeOptions options, ILogger<SystemNode> logger)
    {
        NodeId = options.Id;
        HttpPort = options.HttpPort;
        Peers = options.Peers;
        _leaderId = options.InitialLeaderId;

        logger.LogInformation(
            "Node {NodeId} started. Initial leader is Node {LeaderId}. IsLeader={IsLeader}",
            NodeId,
            _leaderId,
            IsLeader);
    }

    public int NodeId { get; }
    public int HttpPort { get; }
    public IReadOnlyDictionary<int, PeerInfo> Peers { get; }

    public int LeaderId
    {
        get
        {
            lock (_stateLock)
            {
                return _leaderId;
            }
        }
    }

    public bool IsLeader => LeaderId == NodeId;

    public string State
    {
        get
        {
            lock (_stateLock)
            {
                return _state;
            }
        }
    }

    public void SetState(string state)
    {
        lock (_stateLock)
        {
            _state = state;
        }
    }

    public void UpdateLeader(int newLeaderId)
    {
        lock (_stateLock)
        {
            _leaderId = newLeaderId;
            _state = "NORMAL";
        }
    }

    public bool TryGetLeaderUrl(out string leaderUrl)
    {
        leaderUrl = string.Empty;

        if (LeaderId == NodeId)
        {
            return false;
        }

        if (!Peers.TryGetValue(LeaderId, out var peer) || string.IsNullOrWhiteSpace(peer.Url))
        {
            return false;
        }

        leaderUrl = peer.Url.TrimEnd('/');
        return true;
    }
}
