using System.Net.Http.Json;
using CacheNode.Models;

namespace CacheNode.Services;

public sealed class ReplicationService
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly SystemNode _systemNode;
    private readonly NodeOptions _options;
    private readonly ILogger<ReplicationService> _logger;

    public ReplicationService(
        IHttpClientFactory httpClientFactory,
        SystemNode systemNode,
        NodeOptions options,
        ILogger<ReplicationService> logger)
    {
        _httpClientFactory = httpClientFactory;
        _systemNode = systemNode;
        _options = options;
        _logger = logger;
    }

    public async Task<IReadOnlyList<ReplicationResult>> BroadcastUpdateAsync(
        string variableName,
        string newValue,
        long timestamp,
        CancellationToken cancellationToken)
    {
        var request = new CacheUpdateRequest
        {
            SenderNodeId = _systemNode.NodeId,
            VariableName = variableName,
            NewValue = newValue,
            Timestamp = timestamp
        };

        var tasks = _systemNode.Peers.Select(peer => SendForceUpdateAsync(peer.Key, peer.Value, request, cancellationToken));
        return await Task.WhenAll(tasks);
    }

    private async Task<ReplicationResult> SendForceUpdateAsync(
        int peerId,
        PeerInfo peer,
        CacheUpdateRequest request,
        CancellationToken cancellationToken)
    {
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(TimeSpan.FromMilliseconds(_options.RequestTimeoutMs));

            var client = _httpClientFactory.CreateClient("peers");
            var response = await client.PostAsJsonAsync(
                $"{peer.Url.TrimEnd('/')}/force-update",
                request,
                timeout.Token);

            if (response.IsSuccessStatusCode)
            {
                _logger.LogInformation("Replicated {VariableName} to Node {PeerId}.", request.VariableName, peerId);
                return new ReplicationResult(peerId, true);
            }

            return new ReplicationResult(peerId, false, $"HTTP {(int)response.StatusCode}");
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or OperationCanceledException)
        {
            _logger.LogWarning("Replication to Node {PeerId} failed: {Message}", peerId, ex.Message);
            return new ReplicationResult(peerId, false, ex.Message);
        }
    }
}
