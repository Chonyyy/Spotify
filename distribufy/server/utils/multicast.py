# multicast_utils.py
import socket
import struct
import threading
import logging
import time

MULTICAST_GROUP = '224.1.1.1'
MULTICAST_PORT = 5000
DISCOVERY_MESSAGE = b'CHORD_DISCOVERY'

logger = logging.getLogger("__main__")

def send_multicast():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
    
    while True:
        sock.sendto(DISCOVERY_MESSAGE, (MULTICAST_GROUP, MULTICAST_PORT))
        logger.info("Multicast discovery message sent.")
        time.sleep(10)

def receive_multicast():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('', MULTICAST_PORT))
    
    mreq = struct.pack('4sl', socket.inet_aton(MULTICAST_GROUP), socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    
    while True:
        data, addr = sock.recvfrom(1024)
        if data == DISCOVERY_MESSAGE:
            logger.info(f"Discovered node: {addr[0]}")
            return addr[0]  # Return the IP address of the discovered node
