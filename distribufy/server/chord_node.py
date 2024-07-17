import hashlib
import threading
import requests
import time
import logging
import os
from http.server import HTTPServer
from server.utils.my_orm import JSONDatabase
from server.node_reference import ChordNodeReference
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

def get_sha_repr(data: str) -> int:
    """Return SHA-1 hash representation of a string as an integer."""
    return int(hashlib.sha1(data.encode()).hexdigest(), 16)

#region ChordNode
class ChordNode:
    def __init__(self, ip: str, db: JSONDatabase, pred_db: JSONDatabase, succ_db: JSONDatabase, role: str, port: int = 8001, m: int = 160):
        self.id = get_sha_repr(ip)
        self.ip = ip
        self.port = port
        self.role = role
        self.ref = ChordNodeReference(self.id, self.ip, self.port)
        self.succ = self.ref  # Initial successor is itself
        self.pred = self.ref
        self.m = m  # Number of bits in the hash/key space
        self.finger = [self.ref] * self.m  # Finger table
        self.next = 0  # Finger table index to fix next
        self.data = db
        self.replicated_data_pred = pred_db
        self.replicated_data_succ = succ_db
        self.replicating = False#TODO: Is this necesary with thread lock ?
        self.leader = self.ref
        self.election_started = False#TODO: What happens if the election takes too long
        #TODO: If pred or succ changes, replicate the whole database
        server_address = (self.ip, self.port)
        self.httpd = HTTPServer(server_address, ChordNodeRequestHandler)
        self.httpd.node = self
        self.file_storage = f'./databases/node_{self.ip}/files' 
        os.makedirs(self.file_storage, exist_ok=True)
        self.replication_lock = threading.Lock()

        logger.info(f'node_addr: {ip}:{port}')
        
        self.discover_entry()

        # Start server and background threads
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        logger.info(f'HTTP serving commenced')

        threading.Thread(target=self.stabilize, daemon=True).start()  # Start stabilize thread
        threading.Thread(target=self.fix_fingers, daemon=True).start()  # Start fix fingers thread
        threading.Thread(target=self.check_predecessor, daemon=True).start()  # Start check predecessor thread
        threading.Thread(target=self.check_leader, daemon=True).start()  # Start leader election thread
        threading.Thread(target=self.replication_loop, daemon=True).start()  # Start replication thread
          
    #region Data
    
    def replication_loop(self):
        while True:
            time.sleep(30)
            if self.pred.replication_queue:
                with self.replication_lock:
                    self.succ.apply_rep_operations()
                    self.pred.apply_rep_operations()
        
    #TODO: Respond to the callback once the data is actually stored
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
            self.enqueue_replication_operation(data, 'insertion', key)
        else:
            data['key_fields'] = key_fields
            threading.Thread(target=target_node.send_store_data, args= [data, callback, key_fields], daemon=True).start()
            
    def store_file(self, file_key, file_name):
        print(f'Hello {file_key}, {file_name}')#TODO: Implement this with database
            
    def get_data(self, key, callback):#TODO: While testing sending to an invalid url, the url seemed fine but got an error
        logger_dt.info(f'Getting item by key {key}')
        target_node = self.find_succ(key)
        logger.info(f'Asking for key {key}, to node {target_node.ip}|{target_node.id}')
        if target_node.id == self.id:
            ##TODO: respond to callback
            data = self.data.query("id", key)
            self._send_request(callback, data)
        else:
            threading.Thread(target=target_node.send_get_data, args= [key, callback], daemon=True).start()
            
    def store_replic(self, source, data, key):
        logger.info(f'Replic storage comenced')
        if source == self.pred.id:
            logger.info(f'Storing replic information: {data} in from pred node {source}')
            d = {}
            d["id"] = key
            for clave, valor in data.items():
                d[clave] = valor 
            d["source"] = source
            self.replicated_data_pred.insert(d)
            logger.info(f'Replicated data stored')
        if source == self.succ.id:
            logger.info(f'Storing replic information: {data} in from succ node {source}')
            d = {}
            d["id"] = key
            for clave, valor in data.items():
                d[clave] = valor 
            d["source"] = source
            self.replicated_data_succ.insert(d)
            logger.info(f'Replicated data stored')
            
    def enqueue_replication_operation(self, data, operation, key):#TODO: Debug
        max_retries = 5
        retry_interval = 2  # seconds
        
        for _ in range(max_retries):
            if self.succ.id == self.id or self.pred.id == self.id:
                logger.info('Stabilization in progress. Retrying...')
                time.sleep(retry_interval)
            else:
                with self.replication_lock:#TODO: Synch should be implemented so i cant delete something that havent been created ?
                    self.pred.enqueue_rep_operation(self.id, data, operation, key)
                    self.succ.enqueue_rep_operation(self.id, data, operation, key)
                break
        else:
            logger.error('Failed to enqueue replication operation after multiple retries.')
        
    def _debug_log_data(self):
        logger.debug(f'Data in node {self.ip}\n{self.data}\nReplic succ\n{self.replicated_data_succ}\nReplic pred:\n{self.replicated_data_pred}')

    #region Coordination

    def check_leader(self):
        """Regularly check the leader availability."""
        while True and not self.election_started:
            logger_le.info('===CHECKING LEADER===')
            try:
                self.leader.ping
            except requests.ConnectionError:
                logger_le.info('Connection with leader ended')
                self.start_election()
            except Exception as e:
                logger_le.error(f"in checking leader: {e}")
                raise e
            logger_le.info('===CHECKING LEADER===')
            time.sleep(10)
    
    def start_election(self):
        self.election_started = True
        logger_le.info(f'Node {self.id} starting an election.')
        self.leader = None  # Clear current leader
        election_message = {'candidates': [(self.id, self.ip)], 'initiator': (self.id, self.ip)}
        self.succ.send_election_message(election_message)
        
    def process_coordinator_message(self, coordinator_message):
        # self.leader = ChordNodeReference(coordinator_message['id'], coordinator_message['ip'])
        self.election_started = False
        if self.id == coordinator_message['initiator'][0]:
            logger_le.info(f'=== Election Done ===')
            return
        self.leader = ChordNodeReference(coordinator_message['leader'][0], coordinator_message['leader'][1])
        logger_le.info(f'Node {self.id} acknowledges new leader: {(self.leader.id, self.leader.ip)}')
        self.succ.send_coordinator_message(coordinator_message)
        
    def process_election_message(self, election_message):
        if not self.election_started:
            self.election_started = True
        if self.id == election_message['initiator'][0]:
            # Election has completed the ring
            self.determine_leader(election_message['candidates'])
        else:
            # Add self to the list of participating nodes and forward the message
            election_message['candidates'].append((self.id, self.ip))
            self.succ.send_election_message(election_message)

    def determine_leader(self, candidates):
        # Find the candidate with the highest id
        new_leader = max(candidates, key=lambda candidate: candidate[0])
        
        # Update the leader information
        self.leader = ChordNodeReference(new_leader[0], new_leader[1])
        logger_le.info(f'Node {self.id} acknowledged the new leader: {(self.leader.id, self.leader.ip)}')
        
        self.notify_all_nodes(new_leader)
        
    def notify_all_nodes(self, leader):
        """Notify all nodes about the new leader."""
        notification_message = {'leader': (leader[0], leader[1]), 'initiator': (self.id, self.ip)}
        self.succ.send_coordinator_message(notification_message)
    
    #region Chord logic

    def _inbetween(self, k: int, start: int, end: int) -> bool:
        """Check if k is in the interval (start, end]."""
        logger.debug(f'Inbetween (k = {k}, start = {start}, end = {end})')
        if int(start) < int(end):
            return int(start) < int(k) <= int(end)
        else:  # The interval wraps around 0
            return int(start) < int(k) or int(k) <= int(end)

    def find_succ(self, id: int) -> 'ChordNodeReference':
        """Find successor of a given id."""
        node = self.find_pred(id)  # Find predecessor of id
        return node.succ  # Return successor of that node

    def find_pred(self, id: int) -> 'ChordNodeReference':
        """Find predecessor of a given id."""
        node = self
        while not self._inbetween(id, node.id, node.succ.id):
            node = node.closest_preceding_finger(id)
            logger.debug(f'closest preceding finger found {node.id}')
        logger.debug(f'Pred found {node.id}')
        return node

    def closest_preceding_finger(self, id: int) -> 'ChordNodeReference':
        """Find the closest preceding finger for a given id."""
        for i in range(self.m - 1, -1, -1):
            print(i)
            if self.finger[i] and self._inbetween(self.finger[i].id, self.id, id):
                return self.finger[i]
        return self.ref

    def discover_entry(self):
        retries = 4
        retry_interval = 5
        
        logger.info(f"Starting multicast discovery for role: {self.role}")
        discovery_thread = threading.Thread(target=send_multicast, args=(self.role,))
        discovery_thread.daemon = True
        discovery_thread.start()

        for _ in range(retries):
            discovered_ip = receive_multicast(self.role)
            if discovered_ip and discovered_ip != self.ip:
                logger.info(f"Discovered entry point: {discovered_ip}")
                discovered_node = ChordNodeReference(get_sha_repr(discovered_ip), discovered_ip, self.port)
                self.join(discovered_node)
                return
            time.sleep(retry_interval)
        logger.info(f"No other node node discovered.")

    def join(self, node: 'ChordNodeReference'):
        """Join a Chord network using 'node' as an entry point."""
        if node.ip == self.ip:
            return
        logger.info(f'Joining to node {node.id}')
        self.pred = self.ref
        self.succ = node.find_successor(self.id)
        logger.info(f'New-Succ-join | {node.id} | node {self.id}')
        self.succ.notify(self.ref)
        time.sleep(10) #To wait a bit for the ring to stabilize
        self.start_election()

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
                if x.id != self.id and x and self._inbetween(x.id, self.id, self.succ.id):
                    self.succ = x
                    logger.info(f'New-Succ-Stabilize | {x.id} | node {self.id}')
                self.succ.notify(self.ref)
            # except ConnectionRefusedError:
            except requests.ConnectionError:
                self.succ = self.ref
                # self.pred = self.ref#FIXME: THIS WILL CAUSE PROBLEMS IF THERES A PREDECESOR
                logger.info(f'New-Succ-Stabilize | self | node {self.id}')
            except Exception as e:
                logger_stab.error(f"in stabilize: {e}")
            logger_stab.info('===STABILIZING DONE===')
            time.sleep(10)

    def notify(self, node: 'ChordNodeReference'):
        """Notify the node of a change."""
        if node.id != self.id and (not self.pred or self._inbetween(node.id, self.pred.id, self.id)):
            self.pred = node

    def fix_fingers(self):
        """Periodically update finger table entries."""
        while True:
            logger_ff.info('Updating The Finger Table')
            try:
                self.next = (self.next + 1) % self.m
                if self.next == 0:
                    logger_ff.info('Finished Finger Table Iteration')
                    self.print_finger_table()
                self.finger[self.next] = self.find_succ((self.id + 2**self.next) % 2**self.m)
            except Exception as e:
                logger_ff.error(f"Error in fix_fingers: {e}")
            logger_ff.info('===Finger Table Updating Done===')
            time.sleep(10)

    def check_predecessor(self):
        """Periodically check if predecessor is alive."""
        logger_cp.info('Checking Predecessor')
        while True:
            try:
                if self.pred:
                    self.pred.check_predecessor()
            except requests.ConnectionError:#TODO: Debug this code
                self.pred = self.ref
            logger_cp.info('===Predecessor Checking Done===')
            time.sleep(10)
            
    def print_finger_table(self):
        """Print the finger table."""
        logger.debug(f"Finger table for node {self.ip}:{self.port}|{self.id}")
        intervals = []

        for i in range(self.m):
            start = (self.id + 2**i) % 2**self.m
            end = (self.id + 2**(i + 1)) % 2**self.m
            manager_node = self.finger[i]
            intervals.append((start, end, manager_node.ip, manager_node.port))

        # Merge intervals
        merged_intervals = []
        current_start, current_end, current_ip, current_port = intervals[0]

        for start, end, ip, port in intervals[1:]:
            if ip == current_ip and port == current_port:
                current_end = end
            else:
                merged_intervals.append((current_start, current_end, current_ip, current_port))
                current_start, current_end, current_ip, current_port = start, end, ip, port

        merged_intervals.append((current_start, current_end, current_ip, current_port))

        for start, end, ip, port in merged_intervals:
            logger.debug(f"{start}-{end}-{ip}:{port}")
            
    def _send_request(self, url: str, data: dict) -> dict:
        """Send a request and handle retries."""
        max_retries = 4
        for i in range(max_retries):
            response = None
            try:
                logger.info(f'Sending request to {url}\nPayload: {data}')
                response_raw = requests.post(url, json=data)
                response = response_raw.json()#TODO: Remove this after you are done
                logger.debug(f'From {url} received:\n{response}')
                return response
            except requests.ConnectionError as e:
                logger.error(f'Connection Error in IP {self.ip}')
                logger.error(f'{e}')
                if i == max_retries - 1:
                    raise e
            except requests.exceptions.JSONDecodeError as e:
                logger.error(f'JSON Decode Error: {e}')
                logger.error(f'Response text: {response_raw.text}')  # Log the response body
            except Exception as e:
                logger.error(f"Error sending data to {url}: {data}\n{e}")
                raise e