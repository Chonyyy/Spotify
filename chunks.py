import json
from pydub import AudioSegment
import os

def split_audio_file(file_path, chunk_duration_ms=10000):  # 10 segundos por defecto
    sound = AudioSegment.from_mp3(file_path)
    chunks_dir = f"{os.path.splitext(file_path)[0]}_chunks"
    os.makedirs(chunks_dir, exist_ok=True)
    chunks = []
    for i in range(0, len(sound), chunk_duration_ms):
        start_time = i
        end_time = start_time + chunk_duration_ms
        chunk = sound[start_time:end_time]
        chunk_filename = f"{chunks_dir}/chunk_{i}.mp3"
        chunk.export(chunk_filename, format="mp3")
        chunks.append(chunk_filename)
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
    file_path = "/home/chony/Escritorio/Spotify/music/Raffaella Carra – Pedro.mp3"
    chunks = split_audio_file(file_path)
    json_data = create_json_chunks(chunks, file_path)
    save_to_json(json_data, "audio_chunks.json")
    
    print(f"Archivo '{file_path}' dividido en {len(chunks)} chunks y guardado en audio_chunks.json.")

if __name__ == "__main__":
    main()
