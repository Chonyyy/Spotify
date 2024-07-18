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

# Set up logging
logger = logging.getLogger("__main__")
logger_stab = logging.getLogger("__main__.st")
logger_ff = logging.getLogger("__main__.ff")
logger_cp = logging.getLogger("__main__.cp")
logger_rh = logging.getLogger("__main__.rh")
logger_le = logging.getLogger("__main__.le")
logger_dt = logging.getLogger("__main__.dt")

def initialize_database(role, filepath):
    columns = None
    replic_columns = None
    if role == 'gateway':
        columns  = ['id', 'ip', 'role']
        replic_columns = ['id, ip, role']
    return JSONDatabase(filepath, columns), JSONDatabase(filepath + 'pred', replic_columns), JSONDatabase(filepath + 'succ', replic_columns)


def get_sha_repr(data: str) -> int:
    """Return SHA-1 hash representation of a string as an integer."""
    return int(hashlib.sha1(data.encode()).hexdigest(), 16)

#region ChordNode
class GatewayNode(ChordNode):
    def __init__(self, ip: str, db: JSONDatabase, pred_db: JSONDatabase, succ_db: JSONDatabase, role: str, port: int = 8001, m: int = 160):
        self.id = get_sha_repr(ip)
        self.ip = ip
        self.port = port
        self.role = role
        self.ref = ChordNodeReference(self.id, self.ip, self.port)
        self.succ = self.ref
        self.pred = self.ref
        self.m = m  # Number of bits in the hash/key space
        self.finger = [self.ref] * self.m  # Finger table
        self.next = 0  # Finger table index to fix next
        self.data = db
        self.leader = self.ref
        server_address = (self.ip, self.port)
        self.httpd = HTTPServer(server_address, ChordNodeRequestHandler)
        self.httpd.node = self

        logger.info(f'node_addr: {ip}:{port} {self.id}')
        
        self.discover_entry()
        
        self.music_info_leader = self.discover_service('music_info')
        print(self.music_info_leader.ip)
        # self.music_ftp_leader = self.discover_service('music_ftp')

        # Start server and background threads
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        logger.info(f'HTTP serving commenced')

        threading.Thread(target=self.stabilize, daemon=True).start()  # Start stabilize thread
        threading.Thread(target=self.fix_fingers, daemon=True).start()  # Start fix fingers thread
        threading.Thread(target=self.check_predecessor, daemon=True).start()  # Start check predecessor thread
        # threading.Thread(target=self.find_music_info, daemon=True).start()
        # threading.Thread(target=self.find_music_ftp, daemon=True).start() # TODO
        
    def get_songs(self):
        return self.music_info_leader.songs_iterations(self.music_info_leader.id)
        
    def find_music_info(self):
        while True:
            logger_le.info('===CHECKING MUSIC INFO ===')
            try:
                self.leader.ping
            except requests.ConnectionError:
                logger_le.info('Connection with leader ended')
                self.discover_service('music_info')
            except Exception as e:
                logger_le.error(f"in find_music_info: {e}")
                raise e
            logger_le.info('===CHECKING MUSIC INFO===')
            time.sleep(10)
        
    def discover_service(self, role):
        retries = 4
        retry_interval = 5
        
        logger.info(f"Starting multicast discovery for role: {role}")
        discovery_thread = threading.Thread(target=send_multicast, args=(role))
        discovery_thread.daemon = True
        discovery_thread.start()

        for _ in range(retries):
            discovered_ip = receive_multicast(role)
            if discovered_ip:
                logger.info(f"Discovered entry point: {discovered_ip}")
                discovered_node = ChordNodeReference(get_sha_repr(discovered_ip), discovered_ip)
                service_leader = discovered_node.leader
                return ChordNodeReference(service_leader['id'], service_leader['ip'])
            time.sleep(retry_interval)
        logger.info(f"No music_info_node_found")
        return None
        
    def stabilize(self):
        """Regularly check and stabilize the Chord structure."""
        while True:
            logger_stab.info('===STABILIZING===')
            try:
                logger_stab.info(f'Current successor is {self.succ.ip}')
                if self.pred:
                    logger_stab.info(f'Current predecessor is: {self.pred.ip}')
                else:
                    logger_stab.info('Current predecessor is None')

                x = self.succ.pred
                if x.id != self.id and x and self._inbetween(x.id, self.id, self.succ.id):#TODO: replicate all database
                    self.succ = x
                    logger.info(f'New-Succ-Stabilize | {x.ip},{x.id}  | node {self.ip}, {self.ip}')
                    
                self.succ.notify(self.ref)
            except requests.ConnectionError:
                self.succ = self.ref
                logger.info(f'New-Succ-Stabilize | self | node {self.id}')
            except Exception as e:
                logger_stab.error(f"in stabilize: {e}")
            logger_stab.info('===STABILIZING DONE===')
            time.sleep(10)
            
    def notify(self, node: 'ChordNodeReference'):
        """Notify the node of a change."""
        if node.id != self.id and (not self.pred or self._inbetween(node.id, self.pred.id, self.id)):
            self.pred = node
            
    def store_data(self, key_fields, data, callback = None):
        key_information = ''
        for element in [str(data[k]) for k in key_fields]:
            key_information += element
        key = get_sha_repr(key_information)
        logger_dt.info(f'Storing information: {key_information}; key: {key}')
        target_node = self.find_succ(key)
        logger.info(f'Asking for key {key}, to node {target_node.ip}|{target_node.id}')
        if target_node.id == self.id:
            d = {}
            d["id"] = key
            for clave, valor in data.items():
                d[clave] = valor 
                
            self.data.insert(d)
            logger.info(f'Data {key_information} stored at node {self.ip}')
        else:
            data['key_fields'] = key_fields
            threading.Thread(target=target_node.send_store_data, args= [data, callback, key_fields], daemon=True).start()