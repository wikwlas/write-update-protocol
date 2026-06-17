import logging
from typing import Optional
from fastapi import FastAPI, BackgroundTasks, Body, status, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn
import httpx
import threading

from app import config, database, directory_manager, udp_service, election

# Configure console logging formatting
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(threadName)s] %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="Cache Coherence Node 2 - Python")

# Pydantic models for incoming JSON payload validation (Heterogeneous System Contract)
class CacheUpdateRequest(BaseModel):
    senderNodeId: int
    variableName: str
    newValue: str
    timestamp: int = 0

class UserUpdateRequest(BaseModel):
    key: str
    value: str

class ElectionMessage(BaseModel):
    senderNodeId: int
    type: str

@app.on_event("startup")
def startup_event():
    """Spawns asynchronous background UDP services when the FastAPI server initializes."""
    threading.Thread(target=udp_service.udp_sender_thread, daemon=True, name="UdpSender").start()
    threading.Thread(target=udp_service.udp_receiver_thread, args=(election.start_election,), daemon=True, name="UdpReceiver").start()
    logger.info("UDP background services for Node 2 (Python) have been successfully initialized.")

@app.post("/force-update")
async def handle_force_update(request: CacheUpdateRequest):
    """Endpoint triggered exclusively by the Leader node to forcefully broadcast and synchronize cache updates."""
    logger.info(f"Received Write-Update command from Leader: {request.variableName} = {request.newValue}")
    updated = database.local_cache.put(request.variableName, request.newValue, request.timestamp)
    return {"status": "SUCCESS" if updated else "IGNORED_STALE_UPDATE"}

@app.get("/reconstruct-directory")
async def handle_reconstruct_directory():
    """Returns a full dump of the local cache data structure to help a newly elected leader rebuild its global directory."""
    logger.info("Leader requested a localized cache snapshot for structural directory reconstruction.")
    return database.local_cache.get_all()

@app.post("/election")
async def handle_election(message: ElectionMessage, background_tasks: BackgroundTasks):
    """Handles orchestration messages tied to the Bully leader election algorithm."""
    logger.info(f"Bully Protocol: Received {message.type} message from Node {message.senderNodeId}")
    
    if message.type == "ELECTION":
        # If an incoming election signal is from a lower priority node (e.g., Node 1 - .NET), 
        # answer with True to contest, then immediately kick off an assertive internal election loop.
        if config.NODE_ID > message.senderNodeId:
            logger.info(f"Node {message.senderNodeId} has lower priority. Responding with TRUE.")
            background_tasks.add_task(election.start_election)
            return True
        return False
        
    elif message.type == "COORDINATOR":
        logger.info(f"New leader acknowledged across the cluster network: Node {message.senderNodeId}")
        config.CURRENT_LEADER = message.senderNodeId
        config.NODE_STATE = "NORMAL"
        return {"status": "ACK"}

@app.get("/cache/{key}")
async def get_cache_value(key: str):
    """Exposes an interface to check current local cache records via browser or HTTP clients."""
    value = database.local_cache.get(key)
    if value is not None:
        return {"key": key, "value": value}
    return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"error": "Key not found"})

@app.post("/update-request")
async def handle_user_update_request(
    key: Optional[str] = Query(None),
    value: Optional[str] = Query(None),
    request: Optional[UserUpdateRequest] = Body(None),
):
    """
    User-facing endpoint to write or modify transactional records.
    Operates as the authoritative Home Node leader if Python won the most recent Bully election.
    Otherwise, handles proxy delegation routing to the current remote Leader.
    """
    if request is not None:
        key = key or request.key
        value = value if value is not None else request.value

    if not key or value is None:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": "Request requires 'key' and 'value' as JSON fields or query parameters."}
        )

    logger.info(f"Received a local write/update request: {key} = {value}")

    # 1. AUTHENTICATE LEADER ROLE
    if config.CURRENT_LEADER == config.NODE_ID:
        logger.info("Node 2 (Python) is currently acting as LEADER. Committing locally and broadcasting replication payload...")

        write_lock = directory_manager.directory_manager.get_variable_write_lock(key)
        async with write_lock:
            timestamp = directory_manager.directory_manager.next_write_timestamp(key)

            # Mutate local secure cache instance
            database.local_cache.put(key, value, timestamp)
            directory_manager.directory_manager.update_main_memory_value(key, value)
            directory_manager.directory_manager.register_variable_presence(key, config.NODE_ID)

            # Structure standardized JSON payload architecture
            replication_payload = {
                "senderNodeId": config.NODE_ID,
                "variableName": key,
                "newValue": value,
                "timestamp": timestamp
            }

            # Handle distributed write-update broadcasting across known active cluster channels
            async with httpx.AsyncClient() as client:
                for peer_id, peer_info in config.PEERS.items():
                    try:
                        logger.info(f"Replication Engine: Dispatching update down into Node {peer_id} at {peer_info['url']}/force-update")
                        # Dispatched with a non-blocking 1-second circuit-break timeout
                        response = await client.post(
                            f"{peer_info['url']}/force-update",
                            json=replication_payload,
                            timeout=1.0
                        )
                        response.raise_for_status()
                        directory_manager.directory_manager.register_variable_presence(key, peer_id)
                    except Exception as e:
                        logger.warning(f"Failed to cleanly replicate state updates down into Node {peer_id}: {e}")

        return {"status": "SUCCESS", "message": "Variable committed and replicated globally by Leader (Python)."}

    # 2. DELEGATION PROXY ROUTE
    else:
        leader_id = config.CURRENT_LEADER
        leader_url = config.PEERS.get(leader_id, {}).get("url")
        target_url = f"{leader_url}/update-request"

        logger.info(f"Node 2 is currently operating as a Proxy. Forwarding transactional payload to current Leader: {target_url}")

        async with httpx.AsyncClient() as client:
            try:
                # Forward state query strings to the authoritative engine leader (Java/.NET)
                response = await client.post(target_url, json={"key": key, "value": value}, timeout=2.0)

                # Safely parse plaintext string responses out of the foreign Leader cluster
                return JSONResponse(
                    status_code=response.status_code,
                    content={
                        "status": "SUCCESS_VIA_PROXY",
                        "leader_response": response.text
                    }
                )
            except Exception as e:
                return JSONResponse(
                    status_code=500,
                    content={"error": f"Proxy communication link failure with current Leader: {e}"}
                )

@app.get("/status")
async def get_status():
    return {
        "nodeId": config.NODE_ID,
        "leaderId": config.CURRENT_LEADER,
        "isLeader": config.CURRENT_LEADER == config.NODE_ID,
        "state": config.NODE_STATE,
        "cache": database.local_cache.get_all(),
        "directory": directory_manager.directory_manager.snapshot(),
    }

if __name__ == "__main__":
    # Launch application server bounds on local configurations
    uvicorn.run(app, host=config.NODE_IP, port=config.HTTP_PORT)
