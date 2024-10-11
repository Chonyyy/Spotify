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
    # pass
    
    def notify(self, node):
        """Notify the node of a leader change."""
        self._send_request('/gw/notify', {'id': node.id, 'ip': node.ip})
    
    def share_gw_knowledge(self, known_nodes: List['GatewayReference']):
        """Notify the new leader."""
        logger.debug(f'Sharing known gw nodes {known_nodes}')
        nodes = []
        for node in known_nodes:
            nodes.append({'ip': node.ip, 'port': node.port, 'id': node.id})
        self._send_request('/gw/share-gw-knowledge', {'nodes': nodes})

    @property
    def gateway_nodes(self):
        return self._send_request('/gw/gateway-nodes', method='get')