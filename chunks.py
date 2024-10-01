import json
from pydub import AudioSegment
import os

def split_audio_file(file_path, chunk_duration_ms=10000):  # 10 segundos por defecto
    
    chunks = []
    try:
        sound = AudioSegment.from_mp3(file_path)
    
        chunks_dir = f"{os.path.splitext(file_path)[0]}_chunks"
        os.makedirs(chunks_dir, exist_ok=True)
        for i in range(0, len(sound), chunk_duration_ms):
            start_time = i
            end_time = start_time + chunk_duration_ms
            chunk = sound[start_time:end_time]
            chunk_filename = f"{chunks_dir}/chunk_{i}.mp3"
            chunk.export(chunk_filename, format="mp3")
            chunks.append(chunk_filename)
    except:
        pass
    
    return chunks

def create_json_chunks(chunks, file_path):
    json_data = {
        "filename": file_path,
        "chunks": chunks
    }
    return json_data


def save_to_json(json_data, output_file):
    with open(output_file, 'w') as f:
        json.dump(json_data, f, indent=2)

def main():
    all_chunks_songs = []
    for song in [f for f in os.listdir("/home/chony/Escritorio/Spotify/music") if f.lower().endswith(('.mp3', '.mpeg'))]:
        file_path = f"/home/chony/Escritorio/Spotify/music/{song}"
        chunks = split_audio_file(file_path)
        json_data = create_json_chunks(chunks, file_path)
        all_chunks_songs.append(json_data)
        
        print(f"Archivo '{file_path}' dividido en {len(chunks)} chunks y guardado en {os.path.splitext(file_path)[0]}_chunks.json.")
    
    json = {
        "songs_by_chunks": all_chunks_songs
        }
    
    save_to_json(json, f"chunks.json")
    
if __name__ == "__main__":
    main()
