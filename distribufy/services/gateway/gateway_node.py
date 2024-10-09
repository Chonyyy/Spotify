import hashlib
import threading
import time
import logging
import requests
from http.server import HTTPServer
from services.common.multicast import send_multicast, receive_multicast
from typing import List, Tuple
from services.gateway.gateway_reference import GatewayReference
from services.music_service.reference import MusicNodeReference
#TODO: Import storage service reference
from services.common.utils import get_sha_repr
from services.common.chord_node import ChordNode

# Set up logging
logger_gw = logging.getLogger("__main__.gw")


class Gateway(ChordNode):
    def __init__(self, ip: str, port: int = 8001):
        self.ref = GatewayReference(get_sha_repr(ip), ip, port)
        
        # Estructura para guardar nodos conocidos en categorías
        self.known_nodes = {
            'storage_service': None,
            'music_service': None
        }
        self.leader = self.ref
        self.discover_entry()

        self.gateway_nodes = {
            self.ref.id: self.ref
        }
        
        # Stabilizing process
        threading.Thread(target=self.stabilize, daemon=True).start()

        # Check if service nodes are still up
        threading.Thread(target=self.ping_nodes, daemon=True).start()

        # Thread to check if gateway leader is stil up
        threading.Thread(target=self.check_leader, daemon=True).start()  # Start leader election thread

    def check_leader(self):
        """Regularly check the leader availability and manage multicast based on leader status."""
        while True:
            logger_gw.info('===CHECKING LEADER===')
            try:
                if self.leader.id == self.id:
                    # This node is the leader, ensure multicast discovery is running
                    self.multicast_send_toggle(True)

                    # Check for other leaders (only when this node is a leader)
                    other_leader_info = self.discover_other_leader()
                    if other_leader_info and other_leader_info['leader_ip'] != self.ip:
                        logger_gw.info(f"Detected another leader: {other_leader_info['leader_ip']}")

                else:
                    # This node is not the leader, stop multicast discovery
                    self.multicast_send_toggle(False)

                    # Check if the current leader is still alive
                    self.leader.ping
                    logger_gw.info(f'Leader ping succesfull: {self.leader.ip}')

            except requests.ConnectionError:
                del self.gateway_nodes[self.leader.id]
                # Leader is down, start a new election
                logger_gw.info('Connection with leader ended, starting election.')
                self.start_election()

            except Exception as e:
                logger_gw.error(f"Error in leader check: {e}")
            
            logger_gw.info('===LEADER CHECK DONE===')
            time.sleep(10)

    def discover_entry(self):
        retries = 2
        retry_interval = 5

        for _ in range(retries):
            multi_response = receive_multicast(self.role)
            discovered_ip = multi_response[0][0] if multi_response[0] else None
            if discovered_ip and discovered_ip != self.ip:
                logger.info(f"Discovered entry point: {discovered_ip}")
                discovered_node = GatewayReference(get_sha_repr(discovered_ip), discovered_ip, self.port)
                self.leader = discovered_node
                self.join(discovered_node)#TODO: change the join behaviour
                return
            time.sleep(retry_interval)

        logger.info(f"No other node node discovered.")

    def discovery_music_node(self):
        """Discovery for music nodes"""
        retries = 2
        retry_interval = 5

        for _ in range(retries):
            multi_response = receive_multicast("music_service")
            discovered_ip = multi_response[0][0] if multi_response[0] else None
            if discovered_ip and discovered_ip != self.ip:
                logger.info(f"Discovered entry point: {discovered_ip}")
                discovered_node = MusicNodeReference(get_sha_repr(discovered_ip), discovered_ip, self.port)
                self.known_nodes['music_service'] = discovered_node
                return
            time.sleep(retry_interval)
            
    def discovery_ftp_node(self):
        """Discovery for ftp nodes"""
        raise NotImplementedError()

    def start_election(self):
        leader_candidate = self.ref.id
        second_candidate = self.ref.id
        for node in self.gateway_nodes:
            if node.id > leader_candidate:
                second_candidate = leader_candidate
                leader_candidate = node.id
        try:
            new_leader = self.gateway_nodes[leader_candidate]
            new_leader.ping
            self.leader = new_leader
        except requests.ConnectionError:
            del self.gateway_nodes[leader_candidate]
            self.leader = self.gateway_nodes[second_candidate]
        finally:
            #TODO: Notify new leader
            raise NotImplementedError()
                
    def ping_nodes(self):
        """Ping to nodes from each category"""
        for category in self.known_nodes:
            for node in self.known_nodes[category]:
                try:
                    if node:
                        node.ping()
                    else:
                        raise NotImplementedError()
                except requests.ConnectionError:
                    print(f"Node {node.ip} in {category} category is down, removing from the list.")
                    self.known_nodes[category].remove(node)
                    if category == 'music_service':
                        self.discovery_music_node()
                    elif category == 'storage_service':
                        self.discovery_ftp_node()
