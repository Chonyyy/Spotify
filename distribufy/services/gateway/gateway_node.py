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
from services.common.chord_node import ChordNode

# Set up logging
logger = logging.getLogger("__main__")
logger_cl = logging.getLogger("__main__.cl")


class Gateway(ChordNode):
    def __init__(self, ip: str, port: int = 8001):
        super().__init__(ip, None, None, role="gateway", port=port)
        
        # Estructura para guardar nodos conocidos en categorías
        self.known_nodes = {
            'gateway': [],
            'ftp': [],
            'music_service': []
        }
        self.leader = self.ref
        
        # Iniciar multicast para descubrimiento
        self.multicast_msg_event = threading.Event()
        threading.Thread(target=self.discovery_process, daemon=True).start()
        
        # Iniciar proceso de estabilización
        threading.Thread(target=self.stabilize, daemon=True).start()

        # Hilos para ver si los nodos lider ftp y music service siguen vivos
        threading.Thread(target=self.check_leader, daemon=True).start()  # Start leader election thread
        

    def discovery_process(self):
        """Proceso de descubrimiento multicast para añadir nodos a las categorías."""
        while True:
            try:
                # Recibir mensajes multicast
                _, node_info = receive_multicast(self.role)
                if node_info:
                    discovered_node = GatewayReference(node_info['id'], node_info['ip'], node_info['port'])
                    # Asignar nodos a categorías
                    self.add_node_to_category(discovered_node, node_info['role'])
                    if self.leader == self.ref:
                        self.send_multicast()
            except Exception as e:
                print(f"Error in discovery process: {e}")

    def add_node_to_category(self, node_ref, role):
        """Añadir nodos a las categorías correspondientes."""
        if role in self.known_nodes and node_ref not in self.known_nodes[role]:
            self.known_nodes[role].append(node_ref)
            print(f"Node {node_ref.ip} added to {role} category.")

    def send_multicast(self):
        """Enviar mensaje multicast con información del nodo líder y categorías."""
        message = {
            'leader_ip': self.leader.ip,
            'known_nodes': {
                'gateway': [node.ip for node in self.known_nodes['gateway']],
                'ftp': [node.ip for node in self.known_nodes['ftp']],
                'music_service': [node.ip for node in self.known_nodes['music_service']],
            }
        }
        send_multicast(self.role, self.multicast_msg_event, message)

    def stabilize(self):
        """Proceso de estabilización para verificar el estado de los nodos."""
        while True:
            print("Stabilizing...")
            try:
                self.check_leader()
                self.ping_nodes()
            except Exception as e:
                print(f"Error during stabilization: {e}")
            time.sleep(10)

    def check_leader(self):
        """Verificar si el líder sigue activo."""
        if self.leader == self.ref:
            print("I am the leader.")
        else:
            try:
                self.leader.ping
            except Exception:
                print("Leader is down, starting a new election.")
                self.start_election()

    def start_election(self):
        """Proceso de elección de líder."""
        print("Starting leader election...")
        max_node = max(self.known_nodes['gateway'], key=lambda node: node.id)
        if max_node == self.ref:
            self.leader = self.ref
            self.send_multicast()
        else:
            self.leader = max_node

    def ping_nodes(self):
        """Verificar el estado de los nodos en todas las categorías."""
        for category in self.known_nodes:
            for node in self.known_nodes[category]:
                try:
                    node.ping
                except Exception:
                    print(f"Node {node.ip} in {category} category is down, removing from the list.")
                    self.known_nodes[category].remove(node)
