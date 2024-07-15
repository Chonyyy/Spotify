import socket
import pickle
import sys
import math
from os import path

class Client:
    def __init__(self, router_addr) -> None:
        self.song_list = []
        self.router_addr = router_addr

    def refresh_song_list(self):
        ack = tuple(["ACK","OK",None])
        ack_encoded = pickle.dumps(ack)

        try:
            client_sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
            client_sock.connect(self.router_addr)
            songs_list_request = tuple(["RSList", None, None]) #Solicitud para obtener la lista de canciones.
            songs_list_encoded = pickle.dumps(songs_list_request) #La solicitud codificada con pickle.
            client_sock.sendall(songs_list_encoded)
            songs_list_result = client_sock.recv(4096)
            songs_list_decoded = pickle.loads(songs_list_result)
            client_sock.sendall(ack_encoded)

            if 'SSList' in songs_list_decoded:
                self.song_list = songs_list_decoded[1]
                client_sock.close()
                return True
        except Exception as err:
            print(err)
            return False

    def __request_song_known_addr(self, song_id, n_chunks):
        #Solicita una canción dividida en varios "chunks" (trozos) y 
        # guarda cada trozo en la carpeta cache.
        
        ack = tuple(["ACK","OK",None])
        ack_encoded = pickle.dumps(ack)

        try:
            client_sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
            client_sock.connect(self.router_addr)
            rs_request = tuple(["Rsong", song_id, None])
            rs_encoded = pickle.dumps(rs_request)
            client_sock.sendall(rs_encoded)

            for i in range(n_chunks):
                rs_result = client_sock.recv(4096)
                rs_decoded = pickle.loads(rs_result)
                client_sock.sendall(ack_encoded)
                if 'Schunk' in rs_decoded:
                    id_chunk = f'{song_id}_chunk_{i:03}'
                    with open(f'cache/{id_chunk}.mp3', 'wb') as f:
                        f.write(rs_decoded[1])
            client_sock.close()
            return True
        except Exception as err:
            print(err)
            return False

    def request_song(self, song_id, n_chunks):
        return self.__request_song_known_addr(song_id, n_chunks)

    def upload_song(self, song_data, tags):
        if len(tags) != 3:
            print("To upload a song, the tags must be a list with 3 elements.")
            return False

        ack = tuple(["ACK","OK",None])
        ack_encoded = pickle.dumps(ack)

        try:
            client_sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
            client_sock.connect(self.router_addr)
            upload_request = tuple(["NSong", [song_data, tags], None])
            upload_encoded = pickle.dumps(upload_request)
            client_sock.sendall(upload_encoded)

            response = client_sock.recv(4096)
            response_decoded = pickle.loads(response)
            client_sock.sendall(ack_encoded)

            if 'ACK' in response_decoded:
                client_sock.close()
                return True
        except Exception as err:
            print(err)
            return False
        return False

if __name__ == "__main__":
    if len(sys.argv) > 2:
        router_address = (sys.argv[1], int(sys.argv[2]))
    else:
        router_address = ('localhost', 5000)

    cl = Client(router_address)
    print("---------- Welcome: ----------\nTo see the available songs type: 'song list'\nTo add a song to the system: 'add_song <full path(no spaces)>'\nTo request a song type: 'song <id>'",
          "\n where <id> is the number of the desired song. The songs will be saved in /cache.")
    while True:
        print('      ------------------------       ')
        order = input()
        if order == 'song list':
            if cl.refresh_song_list():
                print(cl.song_list)
            else:
                print("Failed to refresh song list.")
        elif "add_song" in order:
            try:
                song_path = order[9:]
                with open(song_path, 'rb') as f:
                    song_bytes = f.read()
                tags = ["Title", "Author", "Genre"]  # Example tags
                if cl.upload_song(song_bytes, tags):
                    print("Song uploaded successfully.")
                else:
                    print("Failed to upload song.")
            except Exception as er:
                print("Error in add_song:", er)
        elif 'song' == order.split()[0]:
            try:
                song_id = int(order.split()[1])
                row = next((ind for ind in cl.song_list if song_id == ind[0]), None)
                if row:
                    duration_sec = row[4] / 1000
                    number_of_chunks = math.ceil(duration_sec / row[5])
                    if cl.request_song(song_id, number_of_chunks):
                        print("Song downloaded successfully.")
                    else:
                        print("Failed to download song.")
            except Exception as er:
                print("Error in song request:", er)
        else:
            print('Wrong request!!')
            print("\nTo see the available songs type: 'song list'\nTo add a song to the system: 'add_song <full path(no spaces)>'\nTo request a song type: 'song <id>'",
                  "\n where <id> is the number of the desired song. The songs will be saved in /cache.\n\n")
