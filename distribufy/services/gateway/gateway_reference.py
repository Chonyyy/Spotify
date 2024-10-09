import hashlib
import requests
import logging
from typing import List
from services.common.node_reference import ChordNodeReference

# Set up logging
logger = logging.getLogger("__main__")
logger_stab = logging.getLogger("__main__.st")
logger_ff = logging.getLogger("__main__.ff")
logger_cp = logging.getLogger("__main__.cp")
logger_rh = logging.getLogger("__main__.rh")
logger_le = logging.getLogger("__main__.le")
logger_dt = logging.getLogger("__main__.dt")

class GatewayReference(ChordNodeReference):
    
    def new_leader(self, leader: 'GatewayReference'):
        """Notify the node of a leader change."""
        self._send_request('/new_leader', {'id': leader.id, 'ip': leader.ip})
    
    def share_knowledge(self, known_nodes: List['GatewayReference']):
        """Notify the new leader."""
        nodes = []
        for node in known_nodes:
            nodes.append({'ip': node.ip, 'port': node.port, 'id': node.id})
        self._send_request('/share_knowledge', {'nodes': nodes})
    
    def ping(self):
        self._send_request('/ping', method='get')
        
    def update_data(self, data):
        """Update the node's data store with the provided data."""
        try:
            payload = {
                'gateway': [],
                'ftp': [],
                'music-manager': []
            }
            for key in payload.keys():
                for reference in data[key]:
                    payload[key].append({'ip':reference.ip, 'port':reference.port})
            self._send_request('/rep_data', {'data': payload})
            logger.info(f"Data replication updated with new data: {payload}")
        except Exception as e:
            logger.error(f'while replicating: {e}')