import os

# Unique identifier for this node
NODE_ID = 2

# IP address for the python machine (MacOS)
NODE_IP = "172.20.10.9"

# Port configurations for this Python node
HTTP_PORT = 8082
UDP_PORT = 4444

PEERS = {
    1: {
        "ip": "172.20.10.9", 
        "url": "http://172.20.10.9:8081",
        "udp_port": 4443
    },  # Node 1: C#
    3: {
        "ip": "172.20.10.8", 
        "url": "http://172.20.10.8:8083"
    }   # Node 3: Java
}

# Global state of the node (using simple variables modified on the fly)
CURRENT_LEADER = 3      # Default leader assigned at startup
NODE_STATE = "NORMAL"   # Changes to "ELECTION" when a leader election is triggered
