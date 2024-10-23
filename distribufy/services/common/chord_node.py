import threading
import requests
import time
import logging
import os
from http.server import HTTPServer
from services.common.my_orm import JSONDatabase
from services.common.utils import get_sha_repr
from services.common.node_reference import ChordNodeReference
from services.common.chord_handler import ChordNodeRequestHandler
from services.common.multicast import send_multicast, receive_multicast
from services.music_service.presentation import MusicNodePresentation
from services.storage_service.storage_handler import StorageRequestHandler

# Set up logging
logger = logging.getLogger("__main__")
logger_stab = logging.getLogger("__main__.st")
logger_ff = logging.getLogger("__main__.ff")
logger_cp = logging.getLogger("__main__.cp")
logger_rh = logging.getLogger("__main__.rh")
logger_le = logging.getLogger("__main__.le")
logger_dt = logging.getLogger("__main__.dt")

chord_references = {
    'gateway': 'class',
    'music_service': 'class',
    'storage_service': 'class',
    'chord_testing': 'class',
}

chord_handlers = {
    'gateway': 'class',
    'music_service': 'class',
    'storage_service': 'class',
    'chord_testing': 'class',
}

#region ChordNode
class ChordNode:
    def __init__(self, ip: str, db: JSONDatabase, sec_succ_db: JSONDatabase, succ_db: JSONDatabase, role: str, port: int = 8001, m: int = 160):
        # Node Information
        self.id = get_sha_repr(ip)
        self.ip = ip
        self.port = port
        self.role = role
        self.ref = ChordNodeReference(self.id, self.ip, self.port)
        # Succ and Pred init
        self.succ = self.ref
        self.sec_succ = self.ref
        self.pred = self.ref
        self.last_pred = self.pred
        self.cached_ips = [
            '172.20.240.2', 
            '172.20.240.4', 
            '172.20.240.6', 
            '172.20.240.8', 
            '172.20.240.10', 
            '172.20.240.12', 
            '172.20.240.14', 
            '172.20.240.16', 
            '172.20.240.18', 
            ]

        # Finger table
        self.m = m  # Number of bits in the hash/key space
        self.finger = [self.ref] * self.m  # Finger table
        self.next = 0  # Finger table index to fix next

        # Data and Replication
        self.data = db
        self.replicated_data_pred = sec_succ_db
        self.replicated_data_sec_pred = succ_db
        self.file_storage = f'./databases/node_{self.ip}/files' 
        os.makedirs(self.file_storage, exist_ok=True)
        self.replication_lock = threading.Lock()
        self.second_succ_replication_lock = threading.Lock()

        # Coordination
        self.leader = self.ref
        self.election_started = False#TODO: What happens if the election takes too long

        # Handler Init
        # server_address = (self.ip, self.port)
        # self.httpd = HTTPServer(server_address, ChordNodeRequestHandler)
        # self.httpd.node = self#TODO: Make it so this is set in ititialization
        
        # Condicionar la inicialización del HTTPServer
        if role == 'chord_testing':  # Solo inicializa httpd si no es clase hija
            server_address = (self.ip, self.port)
            self.httpd = HTTPServer(server_address, ChordNodeRequestHandler)
            self.httpd.node = self
        
        elif role == 'music_service':
            logger.info("Initialize as music service")
            server_address = (self.ip, self.port)
            self.httpd = HTTPServer(server_address, MusicNodePresentation)
            self.httpd.node = self

        elif role == 'storage_service':
            logger.info("Initialize as music service")
            server_address = (self.ip, self.port)
            self.httpd = HTTPServer(server_address, StorageRequestHandler)
            self.httpd.node = self

        logger.info(f'node_addr: {ip}:{port} {self.id}')
        
        # Discovery
        self.multicast_msg_event = threading.Event()
        if not self.discover_entry_cached():
            self.discover_entry()
        # Start server and background threads
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        logger.info(f'HTTP serving commenced')
        self.discovery_thread = threading.Thread(target=send_multicast, args=(self.role, self.multicast_msg_event, {'ip':self.leader.ip, 'id':self.leader.id}), daemon=True)

        threading.Thread(target=self.stabilize, daemon=True).start()  # Start stabilize thread
        threading.Thread(target=self.fix_fingers, daemon=True).start()  # Start fix fingers thread
        threading.Thread(target=self.check_predecessor, daemon=True).start()  # Start check predecessor thread
        threading.Thread(target=self.check_leader, daemon=True).start()  # Start leader election thread
        # threading.Thread(target=self.replication_loop, daemon=True).start()  # Start replication thread
          
    #region Data
    
    def absorb_rep_data(self):
        for entry in self.replicated_data_pred.get_all():
            try:
                if 'second_pred' in entry:
                    del entry['second_pred']
                self.data.insert(entry)
                self.enqueue_replication_operation(entry, 'insertion', entry['key'])
                self.enqueue_replication_operation(entry, 'insertion', entry['key'], True)
            except Exception as e:
                logger.error(f'Error Absorbing pred rep data {e}')

        for entry in self.replicated_data_sec_pred.get_all():
            try:
                if 'second_pred' in entry:
                    del entry['second_pred']
                self.data.insert(entry)
                self.enqueue_replication_operation(entry, 'insertion', entry['key'])
                self.enqueue_replication_operation(entry, 'insertion', entry['key'], True)
            except Exception as e:
                logger.error(f'Error Absorbing sec pred rep data {e}')

        self.replicated_data_pred.drop()
        self.replicated_data_sec_pred.drop()
        return "Done"

    def replication_loop(self):#FIXME: This seems to be causing problems
        # while True:
        #     time.sleep(15)
            logger.info('===Replication Loop Started===')
            try:
                if self.succ.replication_queue:
                    # with self.replication_lock:
                        logger.info(f'apply Succesor rep{self.succ.ip}')
                        self.succ.apply_rep_operations()
                        logger.info(f'Succesor apply rep{self.succ.ip}')
                    # with self.replication_lock:
                    #    logger.info(f'Seccond succesor rep apply{self.sec_succ.ip}')
                        # self.sec_succ.apply_rep_operations()
                if self.sec_succ.replication_queue:
                    # with self.replication_lock:
                        logger.info(f'apply Seccond succesor rep{self.sec_succ.ip}')
                        self.sec_succ.apply_rep_operations()
                        logger.info(f'Seccond succesor rep apply{self.sec_succ.ip}')
                logger.info('===Replication Loop Ended===')
            except Exception as e:
                logger.error(f'ERROR IN REPLICATION LOOP: {e}')
    
    def drop_data(self):
        self.delete_files(self.file_storage)
        
    def replicate_all_database_succ(self):
        logger.info(f'full database replication commenced')
        for record in self.data.get_all():
            record = record.copy()
            key = record['key']
            self.enqueue_replication_operation(record, 'insertion', key)
        logger.info(f'full database replication completed')
        
    def replicate_all_database_sec_succ(self):
        logger.info(f'seccond replication full database replication commenced')
        for record in self.data.get_all():
            record = record.copy()
            key = record['key']
            self.enqueue_replication_operation(record, 'insertion', key, True)
        logger.info(f'seccond replication full database replication completed')

    def drop_sec_suc_rep(self):
        logger.info(f'droping sec succ db')
        self.replicated_data_sec_pred.drop()
    
    def drop_suc_rep(self):
        logger.info(f'droping succ db')
        self.replicated_data_pred.drop()
        
    #TODO: Respond to the callback once the data is actually stored
    def store_data(self, key_fields, data, callback = None):
        key_information = ''.join([str(data[k]) for k in key_fields])
        key = get_sha_repr(key_information)
        logger_dt.info(f'Storing information: {key_information}; key: {key}')

        target_node = self.find_succ(key, 'store_data')
        logger.info(f'Asking for key {key}, to node {target_node.ip}|{target_node.id} condition {target_node.id == self.id}')

        if target_node.id == self.id:
            record = {"key": key, "last_update": "testing", "deleted": False}
            record.update(data)
            logger.debug('STARTING DATA INSERT')
            if 'second_pred' in record:
                del record['second_pred']
            self.data.insert(record)
                
            logger.info(f'Data {key_information} stored at node {self.ip}')

            # threading.Thread(target=self.enqueue_replication_operation, args=(record, 'insertion', key), daemon=True).start()
            logger.debug(f'enqueue replication operation thread started')
            self.enqueue_replication_operation(record, 'insertion', key)
            logger.debug(f'Done Replicating on successor')
            self.enqueue_replication_operation(record, 'insertion', key, True)
            logger.debug(f'Done Replicating')
            
            # Optionally respond to the callback if provided
            if callback:
                self._send_request(callback, {"status": "success", "key": key})
        else:
            # Forward the request to the responsible node
            data['key_fields'] = key_fields
            if callback:
                data['callback'] = callback
            t = threading.Thread(target=target_node.send_store_data, args=(data, callback, key_fields), daemon=True)
            t.start()
            # t.join()
        logger.debug(f'Leaving store data {key}')

    def send_requested_data(self, source_id):
        requested_data = self.data.query(
            'key', 
            ' ', 
            lambda key: 
             (self.id > source_id and int(key) < source_id) or 
             (self.id <= source_id and int(key) < source_id and int(key) > self.id))
        for entry in requested_data:
            self.data.delete('key', entry['key'])
            if 'source' in entry:
                del entry['source']#FIXME Delete these from second succ
        logger.debug(f'Sending {requested_data} to node {source_id}')
        return requested_data

    def get_data(self, key):
        logger_dt.info(f'Getting item by key {key}')
        target_node = self.find_succ(key, 'get_data')
        logger.info(f'Asking for key {key}, to node {target_node.ip}|{target_node.id}')
        if target_node.id == self.id:
            return self.data.query("key", key)
        else:
            data = target_node.send_get_data(key)
            logger.info(f'Returned data is {data}')
            return data
            
    def get_data_target(self, key):
        logger_dt.info(f'Getting item by key {key} in self')
        return self.data.query("key", key)
        
    def store_replic(self, source, data, key, second_pred = False):
        logger.info(f'Replic storage comenced')
        # if source == self.pred.id:
        if not second_pred:
            logger.info(f'Storing replic information: {data} in from pred node {source}')
            d = {}
            d["key"] = key
            for clave, valor in data.items():
                d[clave] = valor 
            if 'second_pred' in d:
                del d['second_pred']
            self.replicated_data_pred.insert(d)
            logger.info(f'Replicated data stored')
        # if source == self.pred.pred('store_replic').id:
        if second_pred:
            logger.info(f'Storing replic information: {data} in from seccond-pred node {source}')
            d = {}
            d["key"] = key
            for clave, valor in data.items():
                d[clave] = valor 
            if 'second_pred' in d:
                del d['second_pred']
            self.replicated_data_sec_pred.insert(d)
            logger.info(f'Replicated data stored')
            
    def enqueue_replication_operation(self, data, operation, key, second_succ = False):
        max_retries = 2
        retry_interval = 1  # seconds
        
        for _ in range(max_retries):
            # if self.succ.id == self.id or self.succ.succ('enqueue_rep_operation') == self.id:
            if (self.succ.id == self.id and not second_succ) or (second_succ and self.sec_succ.id == self.id):# QUe pasa si se intenta replicar mientras se 
                logger.info('Stabilization in progress. Retrying...')
                time.sleep(retry_interval)
            else:
                with self.replication_lock:#TODO: Synch should be implemented so i cant delete something that havent been created ?
                    logger.debug(f'Enqueuing rep op {operation}:{key}')
                    if second_succ:
                        self.sec_succ.enqueue_rep_operation(self.id, data, operation, key, second_pred = second_succ)
                    else:
                        self.succ.enqueue_rep_operation(self.id, data, operation, key)
                    self.replication_loop()
                break
        else:
            logger.info('No successor found')
        
    def _debug_log_data(self):
        logger.debug(f'Data in node {self.ip}\n{self.data}\nReplic succ\n{self.replicated_data_sec_pred}\nReplic pred:\n{self.replicated_data_pred}')

    #region Coordination

    #TODO: Review this
    def merge_rings(self, other_leader_info):
        """Merge the current ring with another ring."""
        logger.info(f"Initiating merge with leader {other_leader_info['leader_ip']}")
        other_leader = ChordNodeReference(other_leader_info['leader_id'], other_leader_info['leader_ip'])
        
        # Notify successor to update the ring structure
        self.succ = other_leader.find_successor(self.id, 'merge_rings')
        self.succ.notify(self.ref)
        
        # Update predecessor as well
        self.pred = other_leader.find_predecessor(self.id, 'merge_rings')
        self.pred.notify(self.ref)
        
        logger.info(f"Rings merged with new successor: {self.succ.ip} and new predecessor: {self.pred.ip}")
        
        # Replicate data across the new ring
        # self.replicate_all_database()
        threading.Thread(target=self.start_election, daemon=True).start()

    def check_leader(self):
        """Regularly check the leader availability and manage multicast based on leader status."""
        while True:
            logger_le.info('===CHECKING LEADER===')
            try:
                if self.leader.id == self.id:
                    # This node is the leader, ensure multicast discovery is running
                    self.multicast_send_toggle(True)

                    # Check for other leaders (only when this node is a leader)
                    other_leader_info = self.discover_other_leader()
                    if other_leader_info and other_leader_info['leader_ip'] != self.ip:
                        logger_le.info(f"Detected another leader: {other_leader_info['leader_ip']}")
                        other_leader = ChordNodeReference(get_sha_repr(other_leader_info['leader_ip']), other_leader_info['leader_ip'])
                        self.join(other_leader)#FIXME
                        # self.merge_rings(other_leader_info)

                else:
                    # This node is not the leader, stop multicast discovery
                    self.multicast_send_toggle(False)

                    # Check if the current leader is still alive
                    self.leader.ping
                    logger_le.info(f'Leader ping succesfull: {self.leader.ip}')

                    if self.succ.id == self.pred.id  == self.id:
                        self.join(self.leader)

            except requests.ConnectionError:
                # Leader is down, start a new election
                logger_le.info('Connection with leader ended, starting election.')
                try:
                    self.start_election()
                except requests.ConnectionError:
                    logger.error(f'Connection error while starting election. Leader is self')
                    self.leader = self.ref

            except Exception as e:
                logger_le.error(f"Error in leader check: {e}")
                # raise e
            
            logger_le.info('===LEADER CHECK DONE===')
            time.sleep(1)

    def discover_other_leader(self):
        """Listen for other leader nodes using multicast."""
        _, other_leader_info = receive_multicast(self.role)
        if other_leader_info and other_leader_info['leader_ip'] != self.ip:
            return other_leader_info
        return None
            
    def leader_info(self):
        return {'ip': self.leader.ip, 'id': self.leader.id}
            
    def get_songs(self):
        local_songs = self.data.get_all().copy()
        local_songs.extend(self.succ.songs_iterations())
            
    def _get_songs(self, origin_id):
        local_songs = self.data.get_all().copy()
        if self.succ.id != origin_id:
            local_songs.extend(self.succ.songs_iterations(origin_id))
        return local_songs
    
    def start_election(self):
        self.election_started = True
        logger_le.info(f'Node {self.id} starting an election.')
        # self.leader = None  # Clear current leader #FIXME
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

    def find_succ(self, id: int, origin = None) -> 'ChordNodeReference':
        """Find successor of a given id."""
        if origin:
            logger.debug(f'find succ origin: {origin}')
        node = self.find_pred(id, origin)  # Find predecessor of id
        if isinstance(node, ChordNode):
            return node.succ
        return node.succ(origin)  # Return successor of that node

    def find_pred(self, id: int, origin = None) -> 'ChordNodeReference':
        """Find predecessor of a given id."""
        if origin:
            logger.debug(f'find pred origin: {origin}')
        node = self
        if self._inbetween(id, node.id, node.succ.id):
            logger.debug(f'Pred found {node.id}{origin}')
            return node
        node = node.closest_preceding_finger(id)
        logger.debug(f'closest preceding finger found {node.id}{origin}')

        while not self._inbetween(id, node.id, node.succ(origin).id):
            node = node.closest_preceding_finger(id)
            logger.debug(f'closest preceding finger found {node.id}{origin}')
        logger.debug(f'Pred found {node.id}{origin}')
        return node

    def closest_preceding_finger(self, id: int) -> 'ChordNodeReference':
        """Find the closest preceding finger for a given id."""
        for i in range(self.m - 1, -1, -1):
            # print(i) TODO: Why ?
            if self.finger[i] and self._inbetween(self.finger[i].id, self.id, id):
                return self.finger[i]
        return self.ref

    def discover_entry(self):
        retries = 2
        retry_interval = 1

        for _ in range(retries):
            multi_response = receive_multicast(self.role)
            discovered_ip = multi_response[0][0] if multi_response[0] else None
            if discovered_ip and discovered_ip != self.ip:
                logger.info(f"Discovered entry point: {discovered_ip}")
                discovered_node = ChordNodeReference(get_sha_repr(discovered_ip), discovered_ip, self.port)
                self.leader = discovered_node
                self.join(discovered_node)
                return
            time.sleep(retry_interval)
        logger.info(f"No other node node discovered.")

    def discover_entry_cached(self):
        for cached_ip in self.cached_ips:
            if cached_ip != self.ip:
                try:
                    cached_node = ChordNodeReference(get_sha_repr(cached_ip), cached_ip, 8001)
                    cached_node.ping
                    if cached_node.role != self.role:
                        logger.info(f'Node with ip {cached_ip} found in different role')
                        continue
                    role_leader_info = cached_node.leader
                    role_leader = ChordNodeReference(role_leader_info['id'], role_leader_info['ip'], 8001)
                    self.leader = role_leader
                    logger.info(f"Discovered entry point: {role_leader.ip}")
                    self.join(role_leader)
                    return True
                except requests.ConnectionError:
                    logger.info(f'Node with ip {cached_ip} not found')
        return False

    
    def multicast_send_toggle(self, enable: bool):
        """Toggle the multicast discovery for the leader node."""
        if enable:
            if not self.discovery_thread.is_alive():
                logger.info(f"Starting multicast discovery for role: {self.role}")
                self.multicast_msg_event.clear()  # Ensure the stop event is cleared
                self.discovery_thread = threading.Thread(
                    target=send_multicast, 
                    args=(self.role, self.multicast_msg_event, {'ip': self.ip, 'id': self.id}), 
                    daemon=True
                )
                self.discovery_thread.start()
            else:
                logger.info("Multicast discovery is already running.")
        else:
            logger.info("Stopping multicast discovery.")
            self.multicast_msg_event.set()  # Trigger the stop event

    def join(self, node: 'ChordNodeReference'):
        """Join a Chord network using 'node' as an entry point."""
        if node.ip == self.ip:
            return
        logger.info(f'Joining to node {node.id}')
        self.pred = self.ref
        self.succ = node.find_successor(self.id, 'join')
        self.sec_succ = self.succ.succ('join')
        if self.sec_succ.id == self.succ.id:
            self.sec_succ = self.ref

        logger.info(f'New-Succ-join | {node.id} | node {self.id}')
        self.succ.notify(self.ref)

        time.sleep(8) #To wait a bit for the ring to stabilize
        data_from_succ = self.succ.request_data(self.id)
        threading.Thread(target=self.request_data_store, args=(data_from_succ,), daemon=True).start()
        self.start_election()

    def request_data_store(self, data_from_succ):
        logger.debug(f'Requested data from succ {len(data_from_succ)}')
        for record in data_from_succ:
            logger.info(record)
            if 'second_pred' in record:
                del record['second_pred']
            logger.info(f'Data {record['key']} stored at node {self.ip}')
            self.data.insert(record)
            self.enqueue_replication_operation(record, 'insertion', record['key'])
            self.enqueue_replication_operation(record, 'insertion', record['key'], True)
            time.sleep(0.5)
            logger.debug('Done enqueue rep request_data')

    def stabilize(self):
        """Regularly check and stabilize the Chord structure."""
        while True:
            logger_stab.info('===STABILIZING===')
            try:
                logger_stab.info(f'Current successor is {self.succ.ip}')
                logger_stab.info(f'Current second successor is {self.sec_succ.ip}')
                if self.pred:
                    logger_stab.info(f'Current predecessor is: {self.pred.ip}')
                else:
                    logger_stab.info('Current predecessor is None')
                    
                x = self.succ.pred('stabilize')
                if x and x.id != self.id and self._inbetween(x.id, self.id, self.succ.id):#TODO: replicate all database
                    self.succ.drop_sec_suc_rep()
                    self.succ.drop_suc_rep()
                    self.succ = x
                    self.sec_succ = x.succ('stabilize1')
                    if self.sec_succ.id == self.succ.id:
                        self.sec_succ = self.ref

                    logger.info(f'New-Succ-Stabilize | {x.ip},{x.id}  | node {self.ip}, {self.ip}')
                    logger.info(f'enqueuing all database')    

                    self.succ.notify(self.ref)

                    if self.succ.id != self.id:
                        logger.info(f'Full replication comenced')
                        self.replicate_all_database_succ()
                        second_succ = self.sec_succ
                        logger.debug(f'succ succ = {second_succ.ip}')
                        if second_succ.id != self.id:
                            self.pred.update_sec_succ(self.succ.id, self.succ.ip)
                            self.pred.replicate_sec_succ()
                        else:
                            self.sec_succ = self.ref
                    
            # except ConnectionRefusedError:
            except requests.ConnectionError:
                self.succ = self.ref
                logger.info(f'New-Succ-Stabilize | self | node {self.id}')
            except Exception as e:
                logger_stab.error(f"in stabilize: {e}")
                raise
            logger_stab.info('===STABILIZING DONE===')
            time.sleep(1)

    def replicate_sec_succ(self):
        self.replicate_all_database_sec_succ()

    def update_sec_succ(self, id, ip):
        logger.info(f'updating sec-succ to {ip}')
        logger.info(f'updating sec-succ to {id}')
        self.sec_succ = ChordNodeReference(id, ip, self.port)#TODO: get also the port
        logger.info(f'updated sec-succ to {ip}')

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
                self.finger[self.next] = self.find_succ((self.id + 2**self.next) % 2**self.m, 'fix_fingers')
            except Exception as e:
                logger_ff.error(f"Error in fix_fingers: {e}")
            logger_ff.info('===Finger Table Updating Done===')
            time.sleep(1)

    def check_predecessor(self):
        """Periodically check if predecessor is alive."""
        logger_cp.info('Checking Predecessor')
        while True:
            try:
                if self.pred:
                    self.pred.ping
            except requests.ConnectionError:
                self.absorb_rep_data()
                logger_cp.info('Predecesor Down')
                self.pred = self.ref
            logger_cp.info('===Predecessor Checking Done===')
            time.sleep(1)
            
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