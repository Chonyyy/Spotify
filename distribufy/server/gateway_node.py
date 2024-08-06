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
from server.handlers.gateway_handler import GatewayRequestHandler

# Set up logging
logger = logging.getLogger("__main__")
logger_cl = logging.getLogger("__main__.cl")

def get_sha_repr(data: str) -> int:
    """Return SHA-1 hash representation of a string as an integer."""
    return int(hashlib.sha1(data.encode()).hexdigest(), 16)


class Gateway:
    def __init__(self, ip: str, port: int = 8001, ip_cache: List[Tuple] = []):
        self.id = get_sha_repr(ip)
        self.ip = ip
        self.port = port
        self.role = 'gateway'
        self.ref = GatewayReference(self.id, self.ip, self.port)
        self.known_nodes = []
        self.leader = None
        server_address = (self.ip, self.port)
        self.httpd = HTTPServer(server_address, GatewayRequestHandler)
        self.httpd.node = self
        self.discovered_node = None
        
        logger.info(f'node_addr: {ip}:{port} {self.id}')
        
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start() # Start server
        threading.Thread(target=self.check_leader, daemon=True).start()  # Start leader election thread
        self.discovery_thread_send = threading.Thread(target=send_multicast, args=(self.role,), daemon=True)
        self.discovery_thread_receive = threading.Thread(target=self.aux_multicast, args=(self.role,), daemon=True)
        self.start_multicast_recieve()
        
        logger.info(f'Threads started')
        
    #region Join Logic
    def set_leader(self):
        while self.leader == None:
            self.leader = GatewayReference(self.id, self.ip, self.port)
    
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
            discovered_ip = receive_multicast('gateway')
            if discovered_ip and discovered_ip != self.ip:
                logger.info(f"Discovered entry point: {discovered_ip}")
                discovered_node = GatewayReference(get_sha_repr(discovered_ip), discovered_ip, self.port)
                self.join(discovered_node)
                return
            time.sleep(retry_interval)
        logger.info(f"No other node node discovered.")
        
    #region Elections
    def set_new_leader(self, new_leader):
        self.leader = new_leader
        
    def share_knowledge(self, nodes):
        self.known_nodes = nodes

    def check_leader(self):
        while True:
            if not self.leader:
                self.leader = self.ref
            else:    
                logger_cl.debug("Leader Checking Initialized")
                try:
                    self.leader.ping()
                except Exception as e: #TODO: Do propper error handling here
                    print(e)
                time.sleep(10)

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
    
    #region Multicast
    def start_multicast_send(self):
        self.discovery_thread_send.start()
            
    def stop_multicast_send(self):
        self.discovery_thread_send.stop()
    
    def start_multicast_recieve(self):
        self.discovery_thread_receive.start()
        time.sleep(10)
        if not self.discovered_node:
            logger.info('Not found leader, i am the leader')
            self.start_multicast_send()
        else:
            logger.info('Found leader:'+ str(self.discovered_node.ip))
            self.leader = self.discovered_node
            self.discovered_node = None
            self.join(self.leader)
        
    def stop_multicast_recieve(self):
        self.discovery_thread_receive.stop()
        
    def aux_multicast(self, role):
        addr = receive_multicast(self.role)
        self.discovered_node = GatewayReference(0, addr[0], 8001)