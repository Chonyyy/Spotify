import hashlib
import threading
import time
import logging
import requests
from typing import List, Tuple
from http.server import HTTPServer
from services.common.multicast import send_multicast, receive_multicast
from services.common.utils import get_sha_repr

from services.common.chord_node import ChordNode
from services.gateway.gateway_reference import GatewayReference
from services.gateway.gateway_handler import GatewayRequestHandler

from services.music_service.reference import MusicNodeReference

# Set up logging
logger_gw = logging.getLogger("__main__.gw")


class Gateway(ChordNode):
    def __init__(self, ip: str, port: int = 8001):
        self.id = get_sha_repr(ip)
        self.role = 'gateway'
        self.ip = ip
        self.port = port
        self.ref = GatewayReference(get_sha_repr(ip), ip, port)
        logger_gw.info(f'{self.id}:{ip}:{port}')
        self.known_gw_nodes_lock = threading.Lock()
        
        # Dict for storing known nodes of each category
        self.known_nodes = {
            'storage_service': None,
            'music_service': None
        }

        # Node for storing other gateway nodes
        self.gateway_nodes = {
            self.ref.id: self.ref
        }

        self.leader = self.ref
        server_address = (self.ip, self.port)
        self.httpd = HTTPServer(server_address, GatewayRequestHandler)
        self.httpd.node = self
        self.discover_entry()
        
        # Start server and background threads
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        logger_gw.info(f'HTTP serving commenced')
        
        # Leader discovery thread
        self.multicast_msg_event = threading.Event()
        self.discovery_thread = threading.Thread(target=send_multicast, args=(self.role, self.multicast_msg_event, {'ip':self.leader.ip, 'id':self.leader.id}), daemon=True)

        # Stabilizing process
        threading.Thread(target=self.stabilize, daemon=True).start()

        # # Check if service nodes are still up
        # threading.Thread(target=self.ping_nodes, daemon=True).start()

        # Thread to check if gateway leader is stil up
        threading.Thread(target=self.check_leader, daemon=True).start()  # Start leader election thread
    
    #region Discovery

    def discover_entry(self):
        retries = 2
        retry_interval = 5

        for _ in range(retries):
            multi_response = receive_multicast(self.role)
            discovered_ip = multi_response[0][0] if multi_response[0] else None
            if discovered_ip and discovered_ip != self.ip:
                logger_gw.info(f"Discovered entry point: {discovered_ip}")
                discovered_node = GatewayReference(get_sha_repr(discovered_ip), discovered_ip, self.port)
                self.leader = discovered_node
                self.join(discovered_node)#TODO: change the join behaviour
                return
            time.sleep(retry_interval)

        logger_gw.info(f"No other node node discovered.")
    
    def stabilize(self):
        while True:
            logger_gw.info(f'===STABILIZATION-CICLE COMENCED===')
            with self.known_gw_nodes_lock:
                logger_gw.debug('Known nodes')
                for node in self.gateway_nodes.values():
                    logger_gw.debug(f'node: {node.ip}')
                for node in self.gateway_nodes.values():
                    try:
                        node.ping
                    except requests.ConnectionError:
                        logger_gw.info(f'node {node.ip} not found')
                        del self.gateway_nodes[node.id]
            logger_gw.info(f'===STABILIZATION-CICLE ENDED===')
            time.sleep(10)

    def check_leader(self):
        """Regularly check the leader availability and manage multicast based on leader status."""
        while True:
            logger_gw.info('===LEADER-CICLE COMMENCED===')
            try:
                if self.leader.id == self.id:
                    logger_gw.info(f'leader is self')
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
            
            logger_gw.info('===LEADER-CICLE ENDED===')
            time.sleep(10)

    # def join(self, node):
    #     """Join a Gateway network using 'node' as an entry point."""
    #     with self.known_gw_nodes_lock:
    #         if node.ip == self.ip or node.id in self.gateway_nodes:
    #             return
    #         logger_gw.info(f'Joining to node {node.ip}:{node.id}')
    #         node.notify(self.ref)
    #         if self.id > node.id:
    #             self.leader = self.ref
    #             self.anounce_leader()
    #         else:
    #             logger_gw.info(f'Setting node {node.ip} ')
    #             self.leader = node
    #             self.gateway_nodes[node.id] = node
    #             self.update_gw_knowledge()

    # def notify_gw(self, node):
    #     self.gateway_nodes[node.id] = node
    #     logger_gw.info(f'New node added {node.ip}')
    #     if node.id > self.leader.id:
    #         logger_gw.info(f'New leader setted {node.ip}')
    #         self.leader = node

    # def anounce_leader(self):
    #     """Iterate all known nodes anouncing itself as leader"""
    #     logger_gw.info(f'Anouncing to all nodes i am the leader')
    #     for node in self.gateway_nodes:
    #         node.notify(self.ref)
    #     logger_gw.info(f'Done anouncing to all nodes i am the leader')

    # def update_gw_knowledge(self):
    #     # get known gw of all nodes
    #     all_known_nodes = {}
    #     for node in self.gateway_nodes.values():
    #         all_known_nodes.update({node.id: node})
    #     # update the knonw gw nodes so they are all up
    #     final_node_dict = {}
    #     for node in all_known_nodes.values():
    #         try:
    #             node.ping
    #             final_node_dict[node['id']] = GatewayReference(node['id'], node['ip'])
    #         except requests.ConnectionError:
    #             continue
    #     # send the new gw dict to all nodes
    #     for node in final_node_dict.values():
    #         node.share_gw_knowledge(final_node_dict)





    # #region Not Checked


    # def discovery_music_node(self):
    #     """Discovery for music nodes"""
    #     retries = 2
    #     retry_interval = 5

    #     for _ in range(retries):
    #         multi_response = receive_multicast("music_service")
    #         discovered_ip = multi_response[0][0] if multi_response[0] else None
    #         if discovered_ip and discovered_ip != self.ip:
    #             logger_gw.info(f"Discovered entry point: {discovered_ip}")
    #             discovered_node = MusicNodeReference(get_sha_repr(discovered_ip), discovered_ip, self.port)
    #             self.known_nodes['music_service'] = discovered_node
    #             return
    #         time.sleep(retry_interval)
            
    # def discovery_ftp_node(self):
    #     """Discovery for ftp nodes"""
    #     raise NotImplementedError()

    # def start_election(self):
    #     leader_candidate = self.ref.id
    #     second_candidate = self.ref.id
    #     for node in self.gateway_nodes:
    #         if node.id > leader_candidate:
    #             second_candidate = leader_candidate
    #             leader_candidate = node.id
    #     try:
    #         new_leader = self.gateway_nodes[leader_candidate]
    #         new_leader.ping
    #         self.leader = new_leader
    #     except requests.ConnectionError:
    #         del self.gateway_nodes[leader_candidate]
    #         self.leader = self.gateway_nodes[second_candidate]
    #     finally:
    #         #TODO: Notify new leader
    #         raise NotImplementedError()
                
    # def ping_nodes(self):
    #     """Ping to nodes from each category"""
    #     for category in self.known_nodes:
    #         nodes = self.known_nodes[category] if self.known_nodes[category] else [] 
    #         if not nodes:
    #             logger_gw.info(f'No known nodes for category {category}')
    #         for node in nodes:
    #             try:
    #                 node.ping()
    #             except requests.ConnectionError:
    #                 logger_gw.info(f"Node {node.ip} in {category} category is down, removing from the list.")
    #                 self.known_nodes[category].remove(node)
    #                 if category == 'music_service':
    #                     self.discovery_music_node()
    #                 elif category == 'storage_service':
    #                     self.discovery_ftp_node()