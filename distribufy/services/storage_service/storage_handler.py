import logging
from services.common.chord_handler import ChordNodeRequestHandler

# Set up logging
logger = logging.getLogger("__main__")

class StorageRequestHandler(ChordNodeRequestHandler):
    def do_POST(self):
        """Handle POST requests, e.g., storing a song fragment."""
        path = self.path

        super().do_POST()
        
        if path == '/store-song-fragment':
            self.handle_store_song_fragment(self.post_data)#TODO: STORE DATA ?

    def do_GET(self):
        """Handle GET requests, e.g., retrieving a song fragment."""
        path = self.path

        super().do_GET()
        
        if path.startswith('/get-song-fragment'):
            self.handle_get_song_fragment()

    def handle_store_song_fragment(self, post_data):
        """Handle storing a song fragment."""
        raise NotImplementedError()

    def handle_get_song_fragment(self):
        """Handle retrieving a song fragment."""
        raise NotImplementedError()
