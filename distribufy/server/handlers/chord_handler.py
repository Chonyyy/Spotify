import hashlib
import threading
import json
import requests
import time
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
from server.handlers.user import User

# Set up logging
logger = logging.getLogger("__main__")
logger_stab = logging.getLogger("__main__.stab")
logger_ff = logging.getLogger("__main__.ff")
logger_cp = logging.getLogger("__main__.cp")
logger_rh = logging.getLogger("__main__.rh")

def get_sha_repr(data: str) -> int:
    """Return SHA-1 hash representation of a string as an integer."""
    return int(hashlib.sha1(data.encode()).hexdigest(), 16)

class ChordNodeRequestHandler(BaseHTTPRequestHandler):

    def do_POST(self):
        """Handle POST requests."""
        content_length = int(self.headers['Content-Length'])
        post_data = json.loads(self.rfile.read(content_length))
        logger_rh.debug(f'Request path {self.path}')
        logger_rh.debug(f'Handling the following request \n{post_data}')
        
        response = None

        if self.path == '/register':
            response = self.handle_register(post_data)
        elif self.path == '/store_user':
            response = self.handle_store_user(post_data)
        elif self.path == '/election':
            response = self.handle_election(post_data)
        elif self.path == '/coordinator':
            response = self.handle_coordinator(post_data)
        elif self.path == '/find_successor':
            response = self.server.node.find_succ(post_data['id'])
        elif self.path == '/find_predecessor':
            response = self.server.node.find_pred(post_data['id'])
        elif self.path == '/get_successor':
            response = self.server.node.succ
        elif self.path == '/get_predecessor':
            response = self.server.node.pred
            logger_rh.debug(f'Response for get_predecessor request:\n{response}')
        elif self.path == '/notify':
            response = self.handle_notify(post_data)
        elif self.path == '/check_predecessor':
            pass  # No action needed for ping
        elif self.path == '/closest_preceding_finger':
            response = self.server.node.closest_preceding_finger(post_data['id'])
        
        self.send_json_response(response)
    
    def do_GET(self):
        """Handle GET requests."""
        logger_rh.debug(f'Request path {self.path}')
        response = None
        
        if self.path.startswith('/get_user'):
            response = self.handle_get_user(self.path)
        elif self.path == '/list_users':
            response = self.server.node.list_users()
        elif self.path == '/debug/finger_table':
            self.server.node._print_finger_table()
        
        self.send_json_response(response, error_message='User not found')

    def handle_register(self, post_data):
        username = post_data['username']
        password = post_data['password']
        user = User(username, password)
        self.server.node.store_user(user)
        return {"status": "success"}

    def handle_store_user(self, post_data):
        username = post_data['username']
        password = post_data['password']
        user = User(username, password)
        self.server.node.data[get_sha_repr(username)] = user.to_dict()
        return {"status": "success"}

    def handle_election(self, post_data):
        self.server.node.process_election_message(post_data)
        return {"status": "success"}

    def handle_coordinator(self, post_data):
        self.server.node.process_coordinator_message(post_data)
        return {"status": "success"}

    def handle_notify(self, post_data):
        node = ChordNodeReference(post_data['id'], post_data['ip'])
        self.server.node.notify(node)

    def handle_get_user(self, path):
        query = path.split('?')
        if len(query) > 1:
            username = query[1].split('=')[1]
            return self.server.node.get_user(username)
        else:
            self.send_response(400)
            self.end_headers()
            return

    def send_json_response(self, response, error_message=None):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()

        if response:
            if isinstance(response, dict):
                self.wfile.write(json.dumps(response).encode())
            elif isinstance(response, ChordNodeReference):
                self.wfile.write(json.dumps({'id': response.id, 'ip': response.ip}).encode())
            else:
                self.wfile.write(json.dumps(response).encode())
        else:
            if error_message:
                self.wfile.write(json.dumps({'status': 'error', 'message': error_message}).encode())
            else:
                self.wfile.write(json.dumps({'id': None, 'ip': None}).encode())

class ChordNodeReference:
    def __init__(self, id: str, ip: str, port: int = 8001):
        self.id = get_sha_repr(ip) if not id else id
        self.ip = ip
        self.port = port
        
    #region Coordination
    def send_election_message(self, election_message):
        self._send_request('/election', election_message)

    def send_coordinator_message(self, coordinator_message):
        self._send_request('/coordinator', coordinator_message)

    #region Buisness logic
    def send_store_user(self, user: User):
        """Send request to store a user."""
        data = {'username': user.username, 'password': user.password}
        self._send_request('/store_user', data)

    #region Chord logic
    def find_successor(self, id: int) -> 'ChordNodeReference':
        """Find successor of a given id."""
        response = self._send_request('/find_successor', {'id': str(id)})
        logger.debug(f'Find Successor Response:\n{response}')
        return ChordNodeReference(response['id'], response['ip'], self.port)
    
    def find_predecessor(self, id: int) -> 'ChordNodeReference':
        """Find predecessor of a given id."""
        response = self._send_request('/find_predecessor', {'id': str(id)})
        logger.debug(f'Find Predecessor Response:\n{response}')
        return ChordNodeReference(response['id'], response['ip'], self.port)
    
    @property
    def succ(self) -> 'ChordNodeReference':
        """Get successor node."""
        response = self._send_request('/get_successor', {})
        logger.debug(f'Get Successor Response:\n{response}')
        return ChordNodeReference(response['id'], response['ip'], self.port)

    @property
    def pred(self) -> 'ChordNodeReference':
        """Get predecessor node."""
        response = self._send_request('/get_predecessor', {})
        logger.debug(f'Get Predecessor Response:\n{response}')
        return ChordNodeReference(response['id'], response['ip'], self.port)

    def notify(self, node: 'ChordNodeReference'):
        """Notify the node of a change."""
        self._send_request('/notify', {'id': node.id, 'ip': node.ip})

    def check_predecessor(self):
        """Ping the predecessor to check if it is alive."""
        self._send_request('/check_predecessor', {})

    def closest_preceding_finger(self, id: int) -> 'ChordNodeReference':
        """Find the closest preceding finger for a given id."""
        response = self._send_request('/closest_preceding_finger', {'id': id})
        return ChordNodeReference(response['id'], response['ip'], self.port)

    #region Utils
    def _send_request(self, path: str, data: dict) -> dict:
        """Send a request and handle retries."""
        max_retries = 10
        for i in range(max_retries):
            try:
                url = f'http://{self.ip}:{self.port}{path}'
                logger.info(f'Sending request to {url}\nPayload: {data}')

                response = requests.post(url, json=data).json()
                logger.debug(f'From {url} received:\n{response}')
                return response
            except requests.ConnectionError:
                logger.error(f'Connection Error in IP {self.ip}')
                if i == max_retries - 1:
                    raise ConnectionError(f'Connection error from IP {self.ip}')
            except Exception as e:
                logger.error(f"Error sending data: {e}")
                raise e
            
    def __str__(self) -> str:
        return f'{self.id},{self.ip},{self.port}'

    def __repr__(self) -> str:
        return str(self)

class ChordNode:
    def __init__(self, ip: str, port: int = 8001, m: int = 160):
        self.id = get_sha_repr(ip)
        self.ip = ip
        self.port = port
        self.ref = ChordNodeReference(self.id, self.ip, self.port)
        self.succ = self.ref  # Initial successor is itself
        self.pred = self.ref
        self.m = m  # Number of bits in the hash/key space
        self.finger = [self.ref] * self.m  # Finger table
        self.next = 0  # Finger table index to fix next
        self.data = {}
        self.leader = self.id
        self.election_started = False#TODO: What happens if the election takes too long

        server_address = (self.ip, self.port)
        self.httpd = HTTPServer(server_address, ChordNodeRequestHandler)
        self.httpd.node = self

        logger.info(f'node_addr: {ip}:{port}')

        # Start server and background threads
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        logger.info(f'HTTP serving commenced')

        threading.Thread(target=self.stabilize, daemon=True).start()  # Start stabilize thread
        threading.Thread(target=self.fix_fingers, daemon=True).start()  # Start fix fingers thread
        threading.Thread(target=self.check_predecessor, daemon=True).start()  # Start check predecessor thread
        
    
    #region Coordination
    
    def start_election(self):
        logger.info(f'Node {self.id} starting an election.')
        self.leader = None  # Clear current leader
        election_message = {'ids': [self.id], 'initiator': self.id}
        self.succ.send_election_message(election_message)
        
    def process_coordinator_message(self, coordinator_message):
        # self.leader = ChordNodeReference(coordinator_message['id'], coordinator_message['ip'])
        self.leader = coordinator_message['id']
        logger.info(f'Node {self.id} acknowledges new leader: {self.leader.id}')
        self.succ.send_coordinator_message(coordinator_message)
        
    def process_election_message(self, election_message):
        if self.id == election_message['initiator']:
            # Election has completed the ring
            self.determine_leader(election_message['ids'])
        else:
            # Add self to the list of participating nodes and forward the message
            election_message['ids'].append(self.id)
            self.succ.send_election_message(election_message)

    def get_higher_nodes(self):
        """Return a list of nodes with higher IDs."""
        # This can be implemented based on your existing node discovery mechanism
        # For example, using the finger table or known nodes
        higher_nodes = [self.finger[i] for i in range(self.m) if self.finger[i].id > self.id]
        return higher_nodes
    
    def determine_leader(self, ids):
        new_leader_id = max(ids)
        logger.info(f'Node {new_leader_id} is elected as the new leader.')
        # self.leader = ChordNodeReference(new_leader_id, self.get_ip(new_leader_id))# Maybe this makes sense
        self.leader = new_leader_id
        logger.info(f'Node {self.id} acknowledged the new leader: {self.leader}')
        self.notify_all_nodes(new_leader_id)
    
    def become_leader(self):
        logger.info(f'Node {self.id} is the new leader.')
        self.leader = self.ref  # Set itself as the leader
        self.notify_all_nodes()
    
    def notify_all_nodes(self, leader_id):
        """Notify all nodes about the new leader."""
        # notification_message = {'id': leader_id, 'ip': self.get_ip(leader_id)}#TODO: Maybe this makes sense
        notification_message = {'id': leader_id}
        self.succ.send_coordinator_message(notification_message)
    
    def get_all_nodes(self):
        """Return a list of all known nodes."""
        # This can be implemented based on your existing node discovery mechanism
        # For example, using the finger table or known nodes
        all_nodes = [self.finger[i] for i in range(self.m)]
        return all_nodes
    
    #endregion Coordination
    
    #region buisness logic

    def store_user(self, user: User):
        """Store user in the appropriate node."""
        user_key = get_sha_repr(user.username)
        logger.debug(f'Storing user: {user_key}')
        target_node = self.find_succ(user_key)
        if target_node.id == self.id:
            self.data[user_key] = user.to_dict()
            logger.info(f'User {user.username} stored at node {self.ip}')
        else:
            target_node.send_store_user(user)

    def get_user(self, username: str) -> dict:
        """Retrieve user from the appropriate node."""
        user_key = get_sha_repr(username)
        target_node = self.find_succ(user_key)
        if target_node.id == self.id:
            return self.data.get(user_key, None)
        else:
            return target_node.send_get_user(username)

    def list_users(self) -> dict:
        """List all users stored in this node."""
        return {"users": list(self.data.values())}
    
    #endregion Buisness logic

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
        return node

    def closest_preceding_finger(self, id: int) -> 'ChordNodeReference':
        """Find the closest preceding finger for a given id."""
        for i in range(self.m - 1, -1, -1):
            if self.finger[i] and self._inbetween(self.finger[i].id, self.id, id):
                return self.finger[i]
        return self.ref

    def join(self, node: 'ChordNodeReference'):
        """Join a Chord network using 'node' as an entry point."""
        if node.ip == self.ip:
            return
        logger.info(f'Joining to node {node.id}')
        self.pred = self.ref
        self.succ = node.find_successor(self.id)
        logger.info(f'New-Succ-join | {node.id} | node {self.id}')
        self.succ.notify(self.ref)
        #FIXME: Election is breaking everything
        # self.start_election()#TODO: Debug this and add start election logic when the leader exits the network

    def stabilize(self):
        """Regularly check and stabilize the Chord structure."""
        while True:
            logger_stab.info('Stabilizing:')
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
            except ConnectionRefusedError:
                self.succ = self.ref
                logger.info(f'New-Succ-Stabilize | self | node {self.id}')
            except Exception as e:
                logger_stab.error(f"Error in stabilize: {e}")
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
                    self._print_finger_table()
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
            except Exception:
                self.pred = None
            logger_cp.info('===Predecessor Checking Done===')
            time.sleep(10)
            
    #region Utils
    def _print_finger_table(self):
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