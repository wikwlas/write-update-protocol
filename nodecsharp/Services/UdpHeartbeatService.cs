using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using CacheNode.Models;

namespace CacheNode.Services;

public sealed class UdpHeartbeatService : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly ConcurrentDictionary<int, long> _lastHeartbeats = new();
    private readonly SystemNode _systemNode;
    private readonly NodeOptions _options;
    private readonly BullyElectionService _electionService;
    private readonly DirectoryManager _directoryManager;
    private readonly ILogger<UdpHeartbeatService> _logger;
    private readonly long _startedAt = TimeProvider.UnixMilliseconds();

    public UdpHeartbeatService(
        SystemNode systemNode,
        NodeOptions options,
        BullyElectionService electionService,
        DirectoryManager directoryManager,
        ILogger<UdpHeartbeatService> logger)
    {
        _systemNode = systemNode;
        _options = options;
        _electionService = electionService;
        _directoryManager = directoryManager;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var receiver = ReceiveHeartbeatsAsync(stoppingToken);
        var sender = SendHeartbeatsAsync(stoppingToken);
        var timeoutChecker = CheckTimeoutsAsync(stoppingToken);
        var startupRecovery = StartupRecoveryAsync(stoppingToken);

        await Task.WhenAll(receiver, sender, timeoutChecker, startupRecovery);
    }

    private async Task StartupRecoveryAsync(CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(1500, cancellationToken);
            await _electionService.SynchronizeFromLeaderAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown.
        }
    }

    private async Task ReceiveHeartbeatsAsync(CancellationToken cancellationToken)
    {
        using var udpClient = new UdpClient(_options.UdpPort);
        _logger.LogInformation("UDP heartbeat receiver listening on port {UdpPort}.", _options.UdpPort);

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var result = await udpClient.ReceiveAsync(cancellationToken);
                var payload = Encoding.UTF8.GetString(result.Buffer);
                var heartbeat = JsonSerializer.Deserialize<HeartbeatMessage>(payload, JsonOptions);

                if (heartbeat is null || heartbeat.NodeId == _systemNode.NodeId)
                {
                    continue;
                }

                _lastHeartbeats[heartbeat.NodeId] = TimeProvider.UnixMilliseconds();
                _logger.LogDebug("Received UDP heartbeat from Node {NodeId}.", heartbeat.NodeId);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (JsonException ex)
            {
                _logger.LogWarning("Invalid heartbeat payload: {Message}", ex.Message);
            }
            catch (SocketException ex)
            {
                _logger.LogWarning("UDP receiver socket error: {Message}", ex.Message);
                await Task.Delay(1000, cancellationToken);
            }
        }
    }

    private async Task SendHeartbeatsAsync(CancellationToken cancellationToken)
    {
        using var udpClient = new UdpClient();

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                if (!_systemNode.State.Equals("ELECTION", StringComparison.OrdinalIgnoreCase))
                {
                    var message = new HeartbeatMessage
                    {
                        NodeId = _systemNode.NodeId,
                        Status = "ALIVE",
                        Timestamp = TimeProvider.UnixMilliseconds()
                    };

                    var bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message, JsonOptions));

                    foreach (var (peerId, peer) in _systemNode.Peers)
                    {
                        var addresses = await Dns.GetHostAddressesAsync(peer.Ip, cancellationToken);
                        if (addresses.Length == 0)
                        {
                            continue;
                        }

                        var endPoint = new IPEndPoint(addresses[0], _options.UdpPort);
                        await udpClient.SendAsync(bytes, bytes.Length, endPoint);

                        _logger.LogDebug("Sent UDP heartbeat to Node {PeerId} at {PeerIp}:{UdpPort}.", peerId, peer.Ip, _options.UdpPort);
                    }
                }

                await Task.Delay(_options.HeartbeatIntervalMs, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex) when (ex is SocketException or FormatException)
            {
                _logger.LogWarning("Could not send UDP heartbeat: {Message}", ex.Message);
                await Task.Delay(_options.HeartbeatIntervalMs, cancellationToken);
            }
        }
    }

    private async Task CheckTimeoutsAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var now = TimeProvider.UnixMilliseconds();

                foreach (var peerId in _systemNode.Peers.Keys)
                {
                    if (_lastHeartbeats.TryGetValue(peerId, out var lastSeen) && now - lastSeen > _options.HeartbeatTimeoutMs)
                    {
                        _lastHeartbeats.TryRemove(peerId, out _);
                        _directoryManager.RemoveNodeFromPresence(peerId);
                        _logger.LogWarning("Node {PeerId} heartbeat timed out.", peerId);

                        if (_systemNode.LeaderId == peerId)
                        {
                            _ = Task.Run(() => _electionService.StartElectionAsync(cancellationToken), cancellationToken);
                        }
                    }
                }

                if (!_systemNode.IsLeader && !_lastHeartbeats.ContainsKey(_systemNode.LeaderId))
                {
                    var startupGracePeriodMs = _options.HeartbeatTimeoutMs + _options.HeartbeatIntervalMs;
                    if (now - _startedAt > startupGracePeriodMs)
                    {
                        _logger.LogWarning("No heartbeat received from initial leader Node {LeaderId}. Starting election.", _systemNode.LeaderId);
                        _ = Task.Run(() => _electionService.StartElectionAsync(cancellationToken), cancellationToken);
                    }
                }

                await Task.Delay(_options.HeartbeatIntervalMs, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }
}
