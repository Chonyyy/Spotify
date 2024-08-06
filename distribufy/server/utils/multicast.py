import socket
import struct
import threading
import logging
import time
import json

MULTICAST_GROUPS = {
    'music_info': '224.1.1.1',
    'music_ftp': '224.1.1.2',
    'gateway': '224.1.1.3'
}
MULTICAST_PORT = 5000
DISCOVERY_MESSAGE = 'CHORD_DISCOVERY'

logger = logging.getLogger("__main__")

def send_multicast(role):
    multicast_group = MULTICAST_GROUPS.get(role)
    logger.info(f'Sending multicas msg in MG {multicast_group}')
    if not multicast_group:
        raise ValueError(f"Invalid role: {role}")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)

    message = json.dumps({'message': DISCOVERY_MESSAGE, 'role': role}).encode('utf-8')

    while True:
        sock.sendto(message, (multicast_group, MULTICAST_PORT))
        logger.info(f"Multicast discovery message sent for role {role}.")
        time.sleep(10)

def receive_multicast(role):
    multicast_group = MULTICAST_GROUPS.get(role)
    logger.info(f'Recieving multicast msg in MG {multicast_group}')
    if not multicast_group:
        raise ValueError(f"Invalid role: {role}")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('', MULTICAST_PORT))

    mreq = struct.pack('4sl', socket.inet_aton(multicast_group), socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    while True:
        data, addr = sock.recvfrom(1024)
        try:
            message = json.loads(data.decode('utf-8'))
            if message['message'] == DISCOVERY_MESSAGE and message['role'] == role:
                logger.info(f"Discovered node: {addr[0]} with role: {message['role']}")
                return addr  # Return the IP address of the discovered node
        except (json.JSONDecodeError, KeyError):
            logger.error(f"Received invalid discovery message: {data}")
