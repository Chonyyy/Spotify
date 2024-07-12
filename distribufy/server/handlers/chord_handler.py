import hashlib
import threading
import json
import requests
import time
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
from server.handlers.user import User

logger = logging.getLogger("__main__")
logger_stab = logging.getLogger("__main__" + ".stab")
logger_ff = logging.getLogger("__main__" + ".ff")
logger_cp = logging.getLogger("__main__" + ".cp")
logger_rh = logging.getLogger("__main__" + ".rh")


# Operation codes (for reference)
FIND_SUCCESSOR = 1
FIND_PREDECESSOR = 2
GET_SUCCESSOR = 3
GET_PREDECESSOR = 4
NOTIFY = 5
CHECK_PREDECESSOR = 6
CLOSEST_PRECEDING_FINGER = 7

def getShaRepr(data: str):
    return int(hashlib.sha1(data.encode()).hexdigest(), 16)

class ChordNodeRequestHandler(BaseHTTPRequestHandler):#TODO: review this class
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = json.loads(self.rfile.read(content_length))
        logger_rh.debug(f'Request path {self.path}')
        logger_rh.debug(f'Handling the following request \n{post_data}')
        
        response = None

        if self.path == '/register':
            username = post_data['username']
            password = post_data['password']
            user = User(username, password)
            self.server.node.store_user(user) #FIXME: This is calling to the node succesor and storing data in it independently it the succesor should store it or not
            response = {"status": "success"}

        elif self.path == '/store_user':
            username = post_data['username']
            password = post_data['password']
            user = User(username, password)
            self.server.node.data[getShaRepr(username)] = user.to_dict()#FIXME: call to a node function
            response = {"status": "success"}
        
        if self.path == '/find_successor':
            response = self.server.node.find_succ(post_data['id'])
        elif self.path == '/find_predecessor':
            response = self.server.node.find_pred(post_data['id'])
        elif self.path == '/get_successor':
            response = self.server.node.succ
        elif self.path == '/get_predecessor':
            response = self.server.node.pred
            logger_rh.debug(f'Response for get_predecesor request:\n{response}')
        elif self.path == '/notify':
            node = ChordNodeReference(post_data['id'], post_data['ip'])
            self.server.node.notify(node)
        elif self.path == '/check_predecessor':
            # No action needed, this is a ping to check if the node is alive
            pass
        elif self.path == '/closest_preceding_finger':
            response = self.server.node.closest_preceding_finger(post_data['id'])

        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()

        if response:
            self.wfile.write(json.dumps({'id': response.id, 'ip': response.ip}).encode())
        else:
            self.wfile.write(json.dumps({'id': None, 'ip': None}).encode())
            
    def do_GET(self):
        logger_rh.debug(f'Request path {self.path}')
        response = None
        
        if self.path.startswith('/get_user'):
            query = self.path.split('?')
            if len(query) > 1:
                username = query[1].split('=')[1]
                response = self.server.node.get_user(username)
            else:
                self.send_response(400)
                self.end_headers()
                return
            
        elif self.path == '/list_users':
            response = self.server.node.list_users()
        
        if self.path == '/debug/finger_table':
            self.server.node._print_finger_table()

        #FIXME: returns status 200 even if an error is found
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()

        if response:
            self.wfile.write(json.dumps(response).encode())
        else:
            self.wfile.write(json.dumps({'status': 'error', 'message': 'User not found'}).encode())


class ChordNodeReference:
    def __init__(self, id: str, ip: str, port: int = 8001):
        self.id = getShaRepr(ip) if not id else id
        self.ip = ip
        self.port = port

    def send_store_user(self, user: User):
        data = {'username': user.username, 'password': user.password}
        self._send_request('/store_user', data)

    def _send_request(self, path: str, data: dict) -> dict:
        try:
            url = f'http://{self.ip}:{self.port}{path}'

            logger.info(f'Sending request to {url}')
            logger.debug(f'Payload: {data}')

            response = (requests.post(url, json= data)).json()
            logger.debug(f'From {url} recieved:\n{response}')
            return response
        except requests.ConnectionError as e:
            raise ConnectionRefusedError(e.strerror)
        except Exception as e:
            logger.error(f"Error sending data: {e}")
            return {}
    
    def find_successor(self, id: int) -> 'ChordNodeReference':
        response = self._send_request('/find_successor', {'id': str(id)})
        logger.debug(f'Find Successor Response:\n{response}')
        return ChordNodeReference(response['id'], response['ip'], self.port)
    
    def find_predecessor(self, id: int) -> 'ChordNodeReference':
        response = self._send_request('/find_predecessor', {'id': str(id)})
        logger.debug(f'Find Predecesor Response:\n{response}')
        return ChordNodeReference(response['id'], response['ip'], self.port)
    
    @property
    def succ(self) -> 'ChordNodeReference':
        response = self._send_request('/get_successor', {})
        logger.debug(f'Get Successor Response:\n{response}')
        return ChordNodeReference(response['id'], response['ip'], self.port)

    @property
    def pred(self) -> 'ChordNodeReference':
        response = self._send_request('/get_predecessor', {})
        logger.debug(f'Get Predecesor Response:\n{response}')
        return ChordNodeReference(response['id'], response['ip'], self.port)

    def notify(self, node: 'ChordNodeReference'):
        self._send_request('/notify', {'id': node.id, 'ip': node.ip})

    def check_predecessor(self):
        self._send_request('/check_predecessor', {})

    def closest_preceding_finger(self, id: int) -> 'ChordNodeReference':
        response = self._send_request('/closest_preceding_finger', {'id': id})
        return ChordNodeReference(response['id'], response['ip'], self.port)

    def __str__(self) -> str:
        return f'{self.id},{self.ip},{self.port}'

    def __repr__(self) -> str:
        return str(self)

    def send_store_user(self, user: User):
        data = {'username': user.username, 'password': user.password}
        self._send_request('/store_user', data)

    def send_get_user(self, username: str) -> dict:
        response = self._send_request('/get_user', {'username': username})
        return response
     
class ChordNode:
    def __init__(self, ip: str, port: int = 8001, m: int = 160):
        self.id = getShaRepr(ip)
        self.ip = ip
        self.port = port
        self.ref = ChordNodeReference(self.id, self.ip, self.port)
        self.succ = self.ref # Initial successor is itself
        # self.pred = None #TODO: Remove this logic
        self.pred = self.ref
        self.m = m # Number of bits in the hash/key space
        self.finger = [self.ref] * self.m # Finger table
        self.next = 0 # Finger table index to fix next
        
        self.data = {}

        server_address = (self.ip, self.port)
        self.httpd = HTTPServer(server_address, ChordNodeRequestHandler)
        self.httpd.node = self

        logger.info(f'node_addr: {ip}:{port}')

        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        logger.info(f'Http serving comenced')

        threading.Thread(target=self.stabilize, daemon=True).start()  # Start stabilize thread
        threading.Thread(target=self.fix_fingers, daemon=True).start()  # Start fix fingers thread
        threading.Thread(target=self.check_predecessor, daemon=True).start()  # Start check predecessor thread

    def store_user(self, user: User):
        user_key = getShaRepr(user.username)
        logger.debug(f'storing user: {user_key}')
        target_node = self.find_succ(user_key)
        if target_node.id == self.id:
            self.data[user_key] = user.to_dict()
            logger.info(f'User {user.username} stored at node {self.ip}')
        else:
            target_node.send_store_user(user)

    def get_user(self, username: str) -> dict:
        user_key = getShaRepr(username)
        target_node = self.find_succ(user_key)
        if target_node.id == self.id:
            return self.data.get(user_key, None)#FIXME
        else:
            return target_node.send_get_user(username)#FIXME

    def list_users(self) -> dict:
        all_users = self.data.values()
        return {"users": list(all_users)}

    def send_get_user(self, username: str) -> dict:
        response = self.ref._send_request('/get_user', {'username': username})#FIXME
        return response

    def list_users(self) -> dict:
        return {"users": list(self.data.values())}

    def _inbetween(self, k: int, start: int, end: int) -> bool:
        """Check if k is in the interval (start, end]."""
        logger.debug(f'inbetween (k = {k}, start = {start}, end = {end})')
        if int(start) < int(end):
            return int(start) < int(k) <= int(end)
        else:  # The interval wraps around 0
            return int(start) < int(k) or int(k) <= int(end)

    def find_succ(self, id: int) -> 'ChordNodeReference':
        node = self.find_pred(id)  # Find predecessor of id
        return node.succ  # Return successor of that node

    def find_pred(self, id: int) -> 'ChordNodeReference':
        node = self
        while not self._inbetween(id, node.id, node.succ.id):
            node = node.closest_preceding_finger(id)
        return node

    def closest_preceding_finger(self, id: int) -> 'ChordNodeReference':
        for i in range(self.m - 1, -1, -1):
            if self.finger[i] and self._inbetween(self.finger[i].id, self.id, id):
                return self.finger[i]
        return self.ref

    def join(self, node: 'ChordNodeReference'):
        """Join a Chord network using 'node' as an entry point."""
        if node:
            self.pred = self.ref
            self.succ = node.find_successor(self.id)
            self.succ.notify(self.ref)
        else:
            self.succ = self.ref
            self.pred = self.ref

    def stabilize(self):
        """Regular check for correct Chord structure."""
        while True:
            logger_stab.info('Stabilizing:')
            try:

                logger_stab.info(f'Current succesor is {self.succ.ip}')
                if self.pred:
                    logger_stab.info(f'Current predecesor is: {self.pred.ip}')
                else:
                    logger_stab.info(f'Current predecesor is None')

                x = self.succ.pred
                if x.id != self.id:
                    if x and self._inbetween(x.id, self.id, self.succ.id):
                        self.succ = x
                    self.succ.notify(self.ref)
            except ConnectionRefusedError as e:
                self.succ = self.ref
            except Exception as e:
                logger_stab.error(f"Error in stabilize: {e}")
            logger_stab.info('===STABILIZING DONE===')
            time.sleep(10)

    def notify(self, node: 'ChordNodeReference'):
        if node.id == self.id:
            pass
        if not self.pred or self._inbetween(node.id, self.pred.id, self.id):
            self.pred = node

    def fix_fingers(self):
        """Periodically update finger table entries."""
        while True:
            logger_ff.info('Updating the finger table')
            try:
                self.next += 1
                if self.next >= self.m:
                    self.next = 0
                self.finger[self.next] = self.find_succ((self.id + 2**self.next) % 2**self.m)
            except Exception as e:
                logger_ff.error(f"Error in fix_fingers: {e}")
            logger_ff.info('===Finger Table Updating Done===')
            time.sleep(10)

    def check_predecessor(self):
        """Periodically check if predecessor is alive."""
        logger_cp.info('Checking Predecesor')
        while True:
            try:
                if self.pred:
                    self.pred.check_predecessor()
            except Exception as e:
                self.pred = None
            logger_cp.info('===Predecesor Checking Done===')
            time.sleep(10)
            
    def _print_finger_table(self):
        logger.debug("Finger table for node {}:{}".format(self.ip, self.port))
        intervals = []

        # Generate intervals
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

        # Print merged intervals
        for start, end, ip, port in merged_intervals:
            logger.debug(f"{start}-{end}-{ip}:{port}")