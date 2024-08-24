import socket
import struct
import threading
import logging
import time
import json

MULTICAST_GROUPS = {
    'music_info': '224.1.1.1',
    'music_ftp': '224.1.1.2',
    # 'gateway': '224.1.1.3',
    'chord_testing': '224.1.1.3',
}
MULTICAST_PORT = 5000
DISCOVERY_MESSAGE = 'CHORD_DISCOVERY'

logger_mc = logging.getLogger("__main__.mc")


def send_multicast(role, stop_event):
    multicast_group = MULTICAST_GROUPS.get(role)
    if not multicast_group:
        raise ValueError(f"Invalid role: {role}")
    
    logger_mc.info(f'Sending multicas msg in MG {multicast_group} for role {role}')

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)

    message = json.dumps({'message': DISCOVERY_MESSAGE, 'role': role}).encode('utf-8')

    while not stop_event.is_set():
        sock.sendto(message, (multicast_group, MULTICAST_PORT))
        logger_mc.info(f"Multicast discovery message sent for role {role}.")
        time.sleep(10)


def receive_multicast(role, timeout=10):
    multicast_group = MULTICAST_GROUPS.get(role)
    if not multicast_group:
        raise ValueError(f"Invalid role: {role}")

    logger_mc.info(f'Receiving multicast messages from {multicast_group} for role {role}')
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('', MULTICAST_PORT))
    
    mreq = struct.pack('4sl', socket.inet_aton(multicast_group), socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    
    sock.settimeout(timeout)
    try:
        while True:
            data, addr = sock.recvfrom(1024)
            try:
                message = json.loads(data.decode('utf-8'))
                if message['message'] == DISCOVERY_MESSAGE and message['role'] == role:
                    logger_mc.info(f"Discovered node: {addr[0]} with role: {message['role']}")
                    return addr  # Return the IP address of the discovered node
            except (json.JSONDecodeError, KeyError):
                logger_mc.error(f"Received invalid discovery message: {data}")
    except socket.timeout:
        logger_mc.info(f"No discovery message received within the timeout of {timeout} seconds.")
        return [None]
    finally:
        sock.close()