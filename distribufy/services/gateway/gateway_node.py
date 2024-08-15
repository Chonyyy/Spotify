import hashlib
import threading
import time
import logging
from http.server import HTTPServer
from services.common.multicast import send_multicast, receive_multicast
from typing import List, Tuple
from services.gateway.gateway_reference import GatewayReference
from services.gateway.gateway_handler import GatewayRequestHandler
from services.common.utils import get_sha_repr


# Set up logging
logger = logging.getLogger("__main__")
logger_cl = logging.getLogger("__main__.cl")


class Gateway:
    def __init__(self, ip: str, port: int = 8001, ip_cache: List[Tuple] = []):
        self.id = get_sha_repr(ip)
        self.ip = ip
        self.port = port
        self.role = 'gateway'
        self.ref = GatewayReference(self.id, self.ip, self.port)
        self.known_nodes = {
            'gateway': [],
            'ftp': [],
            'music-manager': []
        }
        self.rep_data = {}
        self.leader = None
        
        server_address = (self.ip, self.port)
        self.httpd = HTTPServer(server_address, GatewayRequestHandler)
        self.httpd.node = self
        
        self.discovered_node = None
        
        self.replication_lock = threading.Lock()
        self.replication_nodes = []
        
        logger.info(f'node_addr: {ip}:{port}-{self.id}')
        
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start() # Start server
        threading.Thread(target=self.check_leader, daemon=True).start()  # Start leader election thread
        self.stop_discovery_event_send = threading.Event()
        self.discovery_thread_send = threading.Thread(target=send_multicast, args=(self.role, self.stop_discovery_event_send), daemon=True)
        self.stop_discovery_event_rec = threading.Event()
        self.discovery_thread_receive = threading.Thread(target=self.rec_multicast, args=(self.role,self.stop_discovery_event_send), daemon=True)
        threading.Thread(target=self.ckeck_nodes, daemon=True).start()
        
        self.start_multicast_recieve()
        logger.info(f'Threads started')
        
    #region Join Logic
    def set_leader(self):
        while self.leader == None:
            self.leader = GatewayReference(self.id, self.ip, self.port)
    
    def notify(self, node: 'GatewayReference'):
        """Notify the node of a change."""
        if node.id != self.id:
            for known_node in self.known_nodes['gateway']:
                if known_node.id == node.id:
                    return (False, 'already known')
            self.known_nodes['gateway'].append(node)
            if self.id == self.leader.id:
                threading.Thread(target=self.replication, daemon=True).start()
            
            threading.Thread(target=self.start_election, daemon=True).start()
            return (True, 'ok')
        return (False, 'same')
    
    def join(self, node: 'GatewayReference'):
        """Join a Chord network using 'node' as an entry point."""
        logger.info(f'Joining to node network: {node.id}')
        self.leader.notify(self.ref)
        self.leader = node
        
    def discover_entry(self):
        retries = 4
        retry_interval = 5

        for _ in range(retries):
            discovered_ip = receive_multicast('gateway')
            if discovered_ip and discovered_ip != self.ip:
                logger.info(f"Discovered entry point: {discovered_ip}")
                discovered_node = GatewayReference(get_sha_repr(discovered_ip), discovered_ip, self.port)
                self.join(discovered_node)
                self.known_nodes['gateway'].append(discovered_node)
                return
            time.sleep(retry_interval)
        logger.info(f"No other node node discovered.")
        
    #region Elections
    def set_new_leader(self, new_leader):
        self.leader = new_leader
        self.replicate_to_new_leader(new_leader)
        
    def share_knowledge(self, nodes):
        self.known_nodes['gateway'] = nodes

    def check_leader(self):
        while True:
            if not self.leader:
                self.leader = self.ref
            else:    
                logger_cl.debug("Leader Checking Initialized")
                try:
                    if self.leader.id != self.id:
                        self.leader.ping()
                except Exception as e: #TODO: Do propper error handling here
                    print(e)
                time.sleep(10)

    def elect_leader(self, leader):
        self.leader = leader
        for known_node in self.known_nodes['gateway']:
            known_node.new_leader(leader)
        leader.share_knowledge(self.known_nodes)
        self.known_nodes['gateway'] = []

    def start_election(self):
        current_leader = self.ref
        change_leader = False
        for known_node in self.known_nodes['gateway']:
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
            self.stop_discovery_event_rec.set()
        else:
            logger.info('Found leader: '+ str(self.discovered_node.ip))
            self.leader = self.discovered_node
            self.discovered_node = None
            self.join(self.leader)
        
    def stop_multicast_recieve(self):
        self.discovery_thread_receive.stop()
        
    def rec_multicast(self, role, stop_event):
        addr = receive_multicast(role, stop_event)
        if addr[0] == self.ip:
            return
        self.discovered_node = GatewayReference(0, addr[0], 8001)
     
    def ckeck_nodes(self):#TODO: MOve to proper region
        if self.leader.id == self.ref.id: # If i am the leader
            while True:
                for node in self.known_nodes['gateway']:
                    try:
                        node.ping()
                        logger.info(f'Node {node.ip} still connected')
                    except Exception as e: 
                        print(e)
                        self.known_nodes['gateway'].remove(node)
                        logger.info(f'Node {node.ip} is desconected, deleted of the lider known ')
                        threading.Thread(target=self.replication, daemon=True).start()
                time.sleep(10) 
                
    # region Replication
    def replication(self):
        self.get_replication_nodes()
        if self.leader.id == self.id and self.replication_nodes:
            for node in self.replication_nodes:
                try:
                    self.replicate_data(node)
                except Exception as e:
                    logger.error(f"Failed to replicate data to {node.ip}: {e}")
        
    def get_replication_nodes(self):
        sorted_nodes = sorted(self.known_nodes['gateway'], key=lambda node: node.id, reverse=True)
        if len(sorted_nodes) >= 2:
            self.replication_nodes = sorted_nodes[:2]
        elif len(sorted_nodes) == 1:
            self.replication_nodes = sorted_nodes[:1]
        else:
            logger.info('There are no nodes to replicate')
                
    def replicate_data(self, node: 'GatewayReference'):
        """Replicate data to a specific node."""
        with self.replication_lock:
            data = self.known_nodes.copy()  # Make a copy of the in-memory data
            node.update_data(data)  # Suponiendo que el nodo tiene un método `update_data`
            
        logger.info(f"Replicated data to {node.ip}")
        logger.info(f'Data:{node}')

    def replicate_to_new_leader(self, new_leader: 'GatewayReference'):
        """Replicate data to the new leader."""
        if self.leader.id == self.ref.id:  # Si soy el líder actual
            try:
                self.replicate_data(new_leader)
            except Exception as e:
                logger.error(f"Failed to replicate data to the new leader {new_leader.ip}: {e}")

    def update_data(self, data):
        """Update the node's data store with the provided data."""
        with self.replication_lock:
            self.replicate_data.update(data)
        logger.info(f"Data store updated with new data: {data}")
        
    def update_rep_data(self, data):
        self.rep_data = data