using System.Net.Http.Json;
using System.Text.Json;
using CacheNode.Models;
using CacheNode.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
    options.SerializerOptions.PropertyNameCaseInsensitive = true;
});

var nodeOptions = builder.Configuration.GetSection("Node").Get<NodeOptions>() ?? new NodeOptions();

builder.WebHost.UseUrls($"http://{nodeOptions.Ip}:{nodeOptions.HttpPort}");

builder.Services.AddSingleton(nodeOptions);
builder.Services.AddSingleton<SystemNode>();
builder.Services.AddSingleton<LocalCache>();
builder.Services.AddSingleton<DirectoryManager>();
builder.Services.AddSingleton<ReplicationService>();
builder.Services.AddSingleton<BullyElectionService>();
builder.Services.AddHostedService<UdpHeartbeatService>();
builder.Services.AddHttpClient("peers", client =>
{
    client.Timeout = TimeSpan.FromMilliseconds(nodeOptions.RequestTimeoutMs + 500);
});

var app = builder.Build();

app.MapPost("/update-request", async Task<IResult> (
    string key,
    string value,
    SystemNode systemNode,
    LocalCache localCache,
    DirectoryManager directoryManager,
    ReplicationService replicationService,
    IHttpClientFactory httpClientFactory,
    NodeOptions options,
    CancellationToken cancellationToken) =>
{
    if (string.IsNullOrWhiteSpace(key))
    {
        return Results.BadRequest(new { error = "Query parameter 'key' is required." });
    }

    if (!systemNode.IsLeader)
    {
        if (!systemNode.TryGetLeaderUrl(out var leaderUrl))
        {
            return Results.Problem("Leader is unknown or unreachable.", statusCode: StatusCodes.Status503ServiceUnavailable);
        }

        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(TimeSpan.FromMilliseconds(options.RequestTimeoutMs));

            var client = httpClientFactory.CreateClient("peers");
            var escapedKey = Uri.EscapeDataString(key);
            var escapedValue = Uri.EscapeDataString(value);
            var response = await client.PostAsync($"{leaderUrl}/update-request?key={escapedKey}&value={escapedValue}", null, timeout.Token);
            var responseBody = await response.Content.ReadAsStringAsync(timeout.Token);

            return Results.Ok(new
            {
                status = "SUCCESS_VIA_PROXY",
                leaderId = systemNode.LeaderId,
                leaderResponse = responseBody
            });
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or OperationCanceledException)
        {
            return Results.Problem($"Could not forward update to leader Node {systemNode.LeaderId}: {ex.Message}", statusCode: StatusCodes.Status503ServiceUnavailable);
        }
    }

    var timestamp = CacheNode.Services.TimeProvider.UnixMilliseconds();
    return await directoryManager.WithVariableWriteLockAsync<IResult>(key, async () =>
    {
        directoryManager.UpdateMainMemoryValue(key, value);
        directoryManager.RegisterVariablePresence(key, systemNode.NodeId);
        localCache.Put(key, value, timestamp);

        var replicationResults = await replicationService.BroadcastUpdateAsync(key, value, timestamp, cancellationToken);
        foreach (var result in replicationResults.Where(result => result.Success))
        {
            directoryManager.RegisterVariablePresence(key, result.PeerId);
        }

        return Results.Ok(new
        {
            status = "SUCCESS",
            message = "Variable updated and replicated by leader.",
            key,
            value,
            timestamp,
            replicationResults
        });
    });
});

app.MapPost("/force-update", IResult (CacheUpdateRequest request, LocalCache localCache) =>
{
    if (string.IsNullOrWhiteSpace(request.VariableName))
    {
        return Results.BadRequest(new { error = "variableName is required." });
    }

    localCache.Put(request.VariableName, request.NewValue, request.Timestamp);
    return Results.Ok(new { status = "SUCCESS" });
});

app.MapGet("/reconstruct-directory", (LocalCache localCache) =>
{
    return Results.Ok(localCache.Snapshot());
});

app.MapPost("/election", async Task<IResult> (ElectionMessage message, BullyElectionService electionService, CancellationToken cancellationToken) =>
{
    var answered = await electionService.HandleElectionMessageAsync(message, cancellationToken);

    if (message.Type.Equals("ELECTION", StringComparison.OrdinalIgnoreCase))
    {
        return Results.Ok(answered);
    }

    if (answered)
    {
        return Results.Ok(new { status = "ACK" });
    }

    return Results.BadRequest(new { error = "Unsupported election message type." });
});

app.MapGet("/heartbeat", (SystemNode systemNode) =>
{
    return Results.Ok(new HeartbeatMessage
    {
        NodeId = systemNode.NodeId,
        Status = "ALIVE",
        Timestamp = CacheNode.Services.TimeProvider.UnixMilliseconds()
    });
});

app.MapGet("/cache/{key}", IResult (string key, LocalCache localCache) =>
{
    var value = localCache.Get(key);
    if (value is null)
    {
        return Results.NotFound(new { error = "Key not found." });
    }

    return Results.Text(value, "text/plain");
});

app.MapGet("/cache", (LocalCache localCache) => Results.Ok(localCache.Snapshot()));

app.MapGet("/status", (SystemNode systemNode, LocalCache localCache, DirectoryManager directoryManager) =>
{
    return Results.Ok(new
    {
        nodeId = systemNode.NodeId,
        leaderId = systemNode.LeaderId,
        isLeader = systemNode.IsLeader,
        state = systemNode.State,
        peers = systemNode.Peers,
        cache = localCache.Snapshot(),
        directory = directoryManager.Snapshot()
    });
});

app.MapPost("/start-election", async Task<IResult> (BullyElectionService electionService, CancellationToken cancellationToken) =>
{
    await electionService.StartElectionAsync(cancellationToken);
    return Results.Ok(new { status = "ELECTION_STARTED" });
});

app.Run();
