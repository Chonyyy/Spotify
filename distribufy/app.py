import socket
import sys
import logging
import threading
from server.server import start_server
from server.utils.multicast import send_multicast, receive_multicast

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    ip = socket.gethostbyname(socket.gethostname())

    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    date_format = "%H:%M:%S"
    
    logging.basicConfig(
        filename= f'logs_for_{ip}.log',
        format= log_format,
        datefmt= date_format,
        filemode= 'w')
    
    logger.setLevel(logging.INFO)

    discovered_ip = None
    multicast_thread = threading.Thread(target=send_multicast, daemon=True)
    multicast_thread.start()

    if len(sys.argv) >= 2:
        other_ip = sys.argv[1]
        start_server(ip, other_ip)
    else:
        # Attempt to discover another node via multicast
        discovered_ip = receive_multicast()
        if discovered_ip:
            start_server(ip, discovered_ip)
        else:
            start_server(ip)
