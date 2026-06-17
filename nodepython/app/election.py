import httpx
import logging
import asyncio
from app import config, database

logger = logging.getLogger(__name__)

def start_election():
    """Initiates the Bully election algorithm in a separate asynchronous event loop."""
    asyncio.run(start_election_async())

async def start_election_async():
    # If this node is already running an election, ignore the duplicate trigger
    if config.NODE_STATE == "ELECTION":
        return
        
    config.NODE_STATE = "ELECTION"
    logger.info(f"Node {config.NODE_ID} is starting a leader election...")

    # Filter for peer nodes with a HIGHER ID than ours 
    # (For Python [ID:2], the only higher peer is Node 3 [Java])
    higher_nodes = {k: v for k, v in config.PEERS.items() if k > config.NODE_ID}
    
    # If there are no nodes with a higher ID, this node automatically wins
    if not higher_nodes:
        await announce_victory()
        return

    received_answer = False
    async with httpx.AsyncClient() as client:
        for peer_id, peer_info in higher_nodes.items():
            try:
                # Send an ELECTION message to the higher node (Java)
                response = await client.post(
                    f"{peer_info['url']}/election", 
                    json={"senderNodeId": config.NODE_ID, "type": "ELECTION"},
                    timeout=1.0
                )
                # If a higher node responds successfully, it will take over the election
                if response.status_code == 200 and response.json() is True:
                    received_answer = True
            except Exception:
                logger.debug(f"Higher Node {peer_id} did not respond.")

    # If no higher nodes answered, this node assumes leadership
    if not received_answer:
        logger.info(f"No higher nodes responded. Node {config.NODE_ID} (Python) wins the election!")
        await announce_victory()
    else:
        logger.info("A higher node took over the election. Waiting for COORDINATOR message.")
        config.NODE_STATE = "NORMAL"

async def announce_victory():
    config.CURRENT_LEADER = config.NODE_ID
    config.NODE_STATE = "NORMAL"
    logger.info(f"Node {config.NODE_ID} is broadcasting itself as the NEW LEADER.")
    
    # Optional logic: If Python becomes the leader, it could query Java or .NET 
    # to sync/restore the global directory/cache state.
    
    # Broadcast the COORDINATOR message to all active peers
    async with httpx.AsyncClient() as client:
        for peer_id, peer_info in config.PEERS.items():
            try:
                await client.post(
                    f"{peer_info['url']}/election",
                    json={"senderNodeId": config.NODE_ID, "type": "COORDINATOR"},
                    timeout=1.0
                )
            except Exception:
                logger.debug(f"Failed to send COORDINATOR message to Node {peer_id}.")