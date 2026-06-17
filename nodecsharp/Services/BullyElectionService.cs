using System.Net.Http.Json;
using CacheNode.Models;

namespace CacheNode.Services;

public sealed class BullyElectionService
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly SystemNode _systemNode;
    private readonly LocalCache _localCache;
    private readonly DirectoryManager _directoryManager;
    private readonly NodeOptions _options;
    private readonly ILogger<BullyElectionService> _logger;
    private int _electionInProgress;

    public BullyElectionService(
        IHttpClientFactory httpClientFactory,
        SystemNode systemNode,
        LocalCache localCache,
        DirectoryManager directoryManager,
        NodeOptions options,
        ILogger<BullyElectionService> logger)
    {
        _httpClientFactory = httpClientFactory;
        _systemNode = systemNode;
        _localCache = localCache;
        _directoryManager = directoryManager;
        _options = options;
        _logger = logger;
    }

    public async Task StartElectionAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _electionInProgress, 1) == 1)
        {
            _logger.LogInformation("Election is already running. Ignoring duplicate request.");
            return;
        }

        try
        {
            _systemNode.SetState("ELECTION");
            _logger.LogWarning("Node {NodeId} starts Bully election.", _systemNode.NodeId);

            var higherPriorityNodes = _systemNode.Peers
                .Where(peer => peer.Key > _systemNode.NodeId)
                .ToArray();

            if (higherPriorityNodes.Length == 0)
            {
                await AnnounceVictoryAsync(cancellationToken);
                return;
            }

            var responses = await Task.WhenAll(
                higherPriorityNodes.Select(peer => SendElectionMessageAsync(peer.Key, peer.Value, cancellationToken)));

            if (responses.Any(answered => answered))
            {
                _logger.LogInformation("A higher priority node answered. Waiting for COORDINATOR message.");
                _systemNode.SetState("NORMAL");
                return;
            }

            await AnnounceVictoryAsync(cancellationToken);
        }
        finally
        {
            Interlocked.Exchange(ref _electionInProgress, 0);
        }
    }

    public Task<bool> HandleElectionMessageAsync(ElectionMessage message, CancellationToken cancellationToken)
    {
        var type = message.Type.Trim().ToUpperInvariant();

        if (type == "ELECTION")
        {
            if (_systemNode.NodeId > message.SenderNodeId)
            {
                _logger.LogInformation(
                    "Received ELECTION from lower priority Node {Sender}. Answering OK and starting own election.",
                    message.SenderNodeId);

                _ = Task.Run(() => StartElectionAsync(cancellationToken), cancellationToken);
                return Task.FromResult(true);
            }

            return Task.FromResult(false);
        }

        if (type == "COORDINATOR")
        {
            _logger.LogWarning("Node {Sender} announced itself as coordinator.", message.SenderNodeId);
            _systemNode.UpdateLeader(message.SenderNodeId);
            return Task.FromResult(true);
        }

        return Task.FromResult(false);
    }

    public async Task SynchronizeFromLeaderAsync(CancellationToken cancellationToken = default)
    {
        if (_systemNode.IsLeader)
        {
            return;
        }

        if (!_systemNode.TryGetLeaderUrl(out var leaderUrl))
        {
            _logger.LogWarning("Cannot synchronize from leader because leader URL is unknown.");
            return;
        }

        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(TimeSpan.FromMilliseconds(_options.RequestTimeoutMs));

            var client = _httpClientFactory.CreateClient("peers");
            var snapshot = await client.GetFromJsonAsync<Dictionary<string, string>>(
                $"{leaderUrl}/reconstruct-directory",
                timeout.Token);

            if (snapshot is not null)
            {
                _localCache.ReplaceFromSnapshot(snapshot);
                _logger.LogInformation("Recovered local cache from current leader Node {LeaderId}.", _systemNode.LeaderId);
            }
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or OperationCanceledException)
        {
            _logger.LogWarning("Could not recover cache from leader Node {LeaderId}: {Message}", _systemNode.LeaderId, ex.Message);
        }
    }

    private async Task<bool> SendElectionMessageAsync(int peerId, PeerInfo peer, CancellationToken cancellationToken)
    {
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(TimeSpan.FromMilliseconds(_options.RequestTimeoutMs));

            var client = _httpClientFactory.CreateClient("peers");
            var message = new ElectionMessage { SenderNodeId = _systemNode.NodeId, Type = "ELECTION" };
            var response = await client.PostAsJsonAsync($"{peer.Url.TrimEnd('/')}/election", message, timeout.Token);

            if (!response.IsSuccessStatusCode)
            {
                return false;
            }

            // We only send ELECTION to nodes with higher IDs. For safety across the existing
            // heterogeneous implementations, any HTTP 2xx response from such a node counts as
            // proof that a higher-priority node is alive and should take over the election.
            return true;
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or OperationCanceledException)
        {
            _logger.LogDebug("Higher priority Node {PeerId} did not answer election message: {Message}", peerId, ex.Message);
            return false;
        }
    }

    private async Task AnnounceVictoryAsync(CancellationToken cancellationToken)
    {
        _systemNode.UpdateLeader(_systemNode.NodeId);
        _logger.LogWarning("Node {NodeId} is the new leader.", _systemNode.NodeId);

        await ReconstructGlobalDirectoryFromPeersAsync(cancellationToken);

        var coordinatorMessage = new ElectionMessage
        {
            SenderNodeId = _systemNode.NodeId,
            Type = "COORDINATOR"
        };

        var tasks = _systemNode.Peers.Select(peer => NotifyCoordinatorAsync(peer.Key, peer.Value, coordinatorMessage, cancellationToken));
        await Task.WhenAll(tasks);
    }

    private async Task NotifyCoordinatorAsync(
        int peerId,
        PeerInfo peer,
        ElectionMessage coordinatorMessage,
        CancellationToken cancellationToken)
    {
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(TimeSpan.FromMilliseconds(_options.RequestTimeoutMs));

            var client = _httpClientFactory.CreateClient("peers");
            await client.PostAsJsonAsync($"{peer.Url.TrimEnd('/')}/election", coordinatorMessage, timeout.Token);
            _logger.LogInformation("Sent COORDINATOR to Node {PeerId}.", peerId);
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or OperationCanceledException)
        {
            _logger.LogWarning("Could not notify Node {PeerId} about new coordinator: {Message}", peerId, ex.Message);
        }
    }

    private async Task ReconstructGlobalDirectoryFromPeersAsync(CancellationToken cancellationToken)
    {
        _directoryManager.Clear();

        foreach (var (key, value) in _localCache.Snapshot())
        {
            _directoryManager.RegisterVariablePresence(key, _systemNode.NodeId);
            _directoryManager.UpdateMainMemoryValue(key, value);
        }

        var tasks = _systemNode.Peers.Select(peer => PullPeerCacheAsync(peer.Key, peer.Value, cancellationToken));
        var peerSnapshots = await Task.WhenAll(tasks);

        foreach (var peerSnapshot in peerSnapshots.Where(snapshot => snapshot.Cache is not null))
        {
            foreach (var (key, value) in peerSnapshot.Cache!)
            {
                _directoryManager.RegisterVariablePresence(key, peerSnapshot.PeerId);
                _directoryManager.UpdateMainMemoryValue(key, value);
            }
        }

        _logger.LogInformation("Directory reconstruction finished after leader election.");
    }

    private async Task<(int PeerId, Dictionary<string, string>? Cache)> PullPeerCacheAsync(
        int peerId,
        PeerInfo peer,
        CancellationToken cancellationToken)
    {
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(TimeSpan.FromMilliseconds(_options.RequestTimeoutMs));

            var client = _httpClientFactory.CreateClient("peers");
            var cache = await client.GetFromJsonAsync<Dictionary<string, string>>(
                $"{peer.Url.TrimEnd('/')}/reconstruct-directory",
                timeout.Token);

            return (peerId, cache);
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or OperationCanceledException)
        {
            _logger.LogWarning("Could not pull cache snapshot from Node {PeerId}: {Message}", peerId, ex.Message);
            return (peerId, null);
        }
    }
}
