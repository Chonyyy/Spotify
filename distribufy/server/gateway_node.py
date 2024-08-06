import hashlib
import threading
import requests
import time
import logging
import os
from http.server import HTTPServer
from server.utils.my_orm import JSONDatabase
from server.node_reference import ChordNodeReference
from server.chord_node import ChordNode
from server.handlers.chord_handler import ChordNodeRequestHandler
from server.utils.multicast import send_multicast, receive_multicast
from typing import List, Tuple
from server.gateway_reference import GatewayReference

# Set up logging
logger = logging.getLogger("__main__")
logger_stab = logging.getLogger("__main__.st")
logger_ff = logging.getLogger("__main__.ff")
logger_cp = logging.getLogger("__main__.cp")
logger_rh = logging.getLogger("__main__.rh")
logger_le = logging.getLogger("__main__.le")
logger_dt = logging.getLogger("__main__.dt")


def get_sha_repr(data: str) -> int:
    """Return SHA-1 hash representation of a string as an integer."""
    return int(hashlib.sha1(data.encode()).hexdigest(), 16)


class Gateway:
    def __init__(self, ip: str, port: int = 8001, ip_cache: List[Tuple] = []):
        self.id = get_sha_repr(ip)
        self.ip = ip
        self.port = port
        self.ref = GatewayReference(self.id, self.ip, self.port)
        self.known_nodes = []
        self.leader = self.ref
        server_address = (self.ip, self.port)
        self.httpd = HTTPServer(server_address, ChordNodeRequestHandler)
        self.httpd.node = self
        
        logger.info(f'node_addr: {ip}:{port} {self.id}')
        
        self.discover_entry()
        
        # Start server and background threads
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        logger.info(f'HTTP serving commenced')
        
        threading.Thread(target=self.check_leader, daemon=True).start()  # Start leader election thread
        # self.discovery_thread = threading.Thread(target=send_multicast, args=(self.role), daemon=True)
        # self.discovery_thread.start()
        
    def notify(self, node: 'GatewayReference'):
        """Notify the node of a change."""
        if node.id != self.id:
            for known_node in self.known_nodes:
                if known_node.id == node.id:
                    return (False, 'already known')
            self.known_nodes.append(node)
            threading.Thread(target=self.start_election, daemon=True).start()
            return (True, 'ok')
        return (False, 'same')
    
    def set_new_leader(self, new_leader):
        self.leader = new_leader
    
    def share_knowledge(self, nodes):
        self.known_nodes = nodes

    def elect_leader(self, leader):
        self.leader = leader
        for known_node in self.known_nodes:
            known_node.new_leader(leader)
        leader.share_knowledge(self.known_nodes)
        self.known_nodes = []

    def start_election(self):
        current_leader = self.ref
        change_leader = False
        for known_node in self.known_nodes:
            if known_node.id > current_leader.id:
                current_leader = known_node
                change_leader = True
        if change_leader:
            self.elect_leader(current_leader)
        
    def join(self, node: 'GatewayReference'):
        """Join a Chord network using 'node' as an entry point."""
        if node.ip == self.ip:
            return
        logger.info(f'Joining to node {node.id}')
        self.leader = node
        logger.info(f'New-Leader-join | {node.id} | node {self.id}')
        self.leader.notify(self.ref)
        
    def discover_entry(self):
        retries = 4
        retry_interval = 5

        for _ in range(retries):
            discovered_ip = receive_multicast(self.role)
            if discovered_ip and discovered_ip != self.ip:
                logger.info(f"Discovered entry point: {discovered_ip}")
                discovered_node = GatewayReference(get_sha_repr(discovered_ip), discovered_ip, self.port)
                self.join(discovered_node)
                return
            time.sleep(retry_interval)
        logger.info(f"No other node node discovered.")