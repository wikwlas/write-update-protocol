import socket
import json
import time
import threading
import logging
from app import config

logger = logging.getLogger(__name__)

# Dictionary tracking the last known contact time: {node_id: timestamp_ms}
LAST_HEARTBEATS = {}  

def udp_sender_thread():
    """Broadcasts UDP Heartbeat packages every 2 seconds to configured peers."""
    # Initialize a low-level UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    while True:
        # Pause heartbeats if this node is currently engaging in a Bully election
        if config.NODE_STATE != "ELECTION":
            payload = {
                "nodeId": config.NODE_ID,
                "status": "ALIVE",
                "timestamp": int(time.time() * 1000)
            }
            message = json.dumps(payload).encode('utf-8')
            
            for peer_id, peer_info in config.PEERS.items():
                try:
                    target_udp_port = peer_info.get("udp_port", config.UDP_PORT)
                    sock.sendto(message, (peer_info["ip"], target_udp_port))
                except Exception as e:
                    logger.debug(f"Failed to transmit UDP heartbeat to Node {peer_id}: {e}")
                    
        time.sleep(2)

def udp_receiver_thread(start_election_callback):
    """Listens for incoming UDP heartbeats and orchestrates peer timeout checks."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((config.NODE_IP, config.UDP_PORT))
    sock.settimeout(1.0)  # Prevents the read operation from blocking the thread infinitely
    
    # Internal daemon thread dedicated exclusively to checking leader timeouts
    def timeout_checker():
        while True:
            current_time = time.time() * 1000
            
            # If the current active leader fails to make contact within 5 seconds -> Trigger election!
            leader_last_contact = LAST_HEARTBEATS.get(config.CURRENT_LEADER)
            if leader_last_contact and (current_time - leader_last_contact) > 5000:
                if config.NODE_STATE != "ELECTION":
                    logger.warning(f"Leader timeout detected (Node {config.CURRENT_LEADER}). Launching Bully algorithm...")
                    LAST_HEARTBEATS.clear()
                    # Spin up the election process on a separate background thread
                    threading.Thread(target=start_election_callback).start()
            time.sleep(2)
            
    threading.Thread(target=timeout_checker, daemon=True).start()

    while True:
        try:
            data, addr = sock.recvfrom(1024)
            payload = json.loads(data.decode('utf-8'))
            sender_id = payload["nodeId"]
            
            # Record or update the timestamp for the sender node
            LAST_HEARTBEATS[sender_id] = time.time() * 1000
        except socket.timeout:
            continue
        except Exception as e:
            logger.error(f"UDP Receiver Exception occurred: {e}")
