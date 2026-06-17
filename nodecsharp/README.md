# Node 1 - C#/.NET cache coherence node

This folder contains the Linux/WSL C# implementation of Node 1 for the directory-based Write-Update cache coherence project.

## Assumptions

1. Node IDs follow the assignment: C# = 1, Python = 2, Java = 3.
2. HTTP endpoints match the existing Java and Python code:
   - `POST /update-request?key=<name>&value=<value>`
   - `POST /force-update`
   - `POST /election`
   - `GET /reconstruct-directory`
   - `GET /cache/{key}`
3. UDP is used for heartbeat messages, because the professor feedback points out that HTTP polling is too expensive for liveness checks.
4. Values are stored as strings to avoid cross-language numeric/type-conversion problems.
5. The leader serializes writes per variable. If two nodes write the same variable at the same time, the leader processes them in order and the later accepted timestamp wins on followers.
6. For the demo, a leader broadcasts updates to every configured peer. This matches the existing Java/Python code and the test expectation that a write from Node 1 is visible on Node 2.

## Configure

Edit `appsettings.json` before running on real machines:

```json
"Peers": {
  "2": { "Ip": "<python-mac-ip>", "Url": "http://<python-mac-ip>:8082" },
  "3": { "Ip": "<java-windows-ip>", "Url": "http://<java-windows-ip>:8083" }
}
```

Keep `Node.Id` as `1` and `Node.HttpPort` as `8081` for the C# node.

## Build and run on WSL

```bash
cd nodecsharp
dotnet restore
dotnet build
dotnet run
```

The node listens on `http://0.0.0.0:8081` by default.

## Quick local tests

Check status:

```bash
curl http://localhost:8081/status
```

Write through the cluster:

```bash
curl -X POST "http://localhost:8081/update-request?key=x&value=123"
```

Read local cache:

```bash
curl http://localhost:8081/cache/x
```

Force a local update without a leader, useful for testing only:

```bash
curl -X POST http://localhost:8081/force-update \
  -H "Content-Type: application/json" \
  -d '{"senderNodeId":3,"variableName":"x","newValue":"999","timestamp":1730000000000}'
```

Manually trigger election:

```bash
curl -X POST http://localhost:8081/start-election
```

## Notes for one-machine testing

The supplied Java and Python nodes use UDP port `4444`. That works when the nodes run on separate computers or separate VMs. If all nodes run on the same OS instance, only one process can normally bind the same UDP port, so use separate VMs/containers or change the UDP configuration consistently in all three nodes.
