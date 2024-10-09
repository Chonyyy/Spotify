# Commands

Commands to taste the spotify back-end

## Building

docker build -t distribufy .

## Running a container instance

sudo docker run -it --rm --name first_container -p 8001:8001 -v .\:/app spotify
sudo docker run -it --rm --name second_container -p 8002:8001 -v .\:/app spotify
sudo docker run -it --rm --name third_container -p 8003:8001 -v .\:/app spotify
sudo docker run -it --rm --name fourth_container -p 8004:8001 -v .\:/app spotify
sudo docker run -it --rm --name fifth_container -p 8005:8001 -v .\:/app spotify

## Comandos de CHordNode

### Inicializar un nodo de role chord

``` bash
python app.py chord_testing
```

### Store Data

ubuntu

``` bash
curl -X POST http://localhost:8002/store-data \
     -H "Content-Type: application/json" \
     -d "{\"key_fields\":[\"title\"],\"title\":\"t1\", \"album\":\"ab\", \"genre\":\"g1\", \"artist\":\"a2\"}"
```

windows

``` bash
curl -X POST http://localhost:8002/store-data ^
     -H "Content-Type: application/json" ^
     -d "{\"key_fields\":[\"title\"],\"title\":\"t1\", \"album\":\"a\", \"genre\":\"g\", \"artist\":\"a\", \"callback\": \"http://a.com\"}"
```

### Getting Data

Invoke-WebRequest -Uri http://localhost:8002/get-data -Method POST -Body '{"callback":"https://warever.com", "key":922182856952740759111154525263473632235391720628}' -ContentType "application/json"

curl -X POST http://localhost:8002/get-data ^
     -H "Content-Type: application/json" ^
     -d '{"callback":"https://warever.com", "key":922182856952740759111154525263473632235391720628}'

### Debug Data

ubuntu

``` bash
curl -X GET http://localhost:8001/debug-node-data \
     -H "Content-Type: application/json" \
     -d '{}'
```

windows

``` bash
curl -X GET http://localhost:8001/debug-node-data ^
     -H "Content-Type: application/json" ^
     -d "{}"
```

### Getting an specific user

Invoke-WebRequest -Uri http://localhost:8001/get_user?user=user1 -Method GET -ContentType "application/json"
curl -X GET http://localhost:8001/get_user?user=user1 \
     -H "Content-Type: application/json"

### Getting all Users

Invoke-WebRequest -Uri http://localhost:8001/list_users -Method GET -ContentType "application/json"
curl -X GET http://localhost:8001/list_users \
     -H "Content-Type: application/json"

### Printing the Finger table

Invoke-WebRequest -Uri http://localhost:8001/debug/finger_table -Method GET -ContentType "application/json"
curl -X GET http://localhost:8001/debug/finger_table \-H "Content-Type: application/json"

curl -X POST http://localhost:8001/store-data \
     -H "Content-Type: application/json" \
     -d '{"username": "user1", "password": "password123", "callback":"warever.com", "key_fields":["username"]}'

curl -X GET http://localhost:8001/debug/finger_table \
     -H "Content-Type: application/json"

## Comandos de MusicNode

### Inicializar un nodo de role music node

``` bash
python app.py music_service
```

### Get data base of songs

ubuntu

``` bash
curl -X GET http://localhost:8002/get-db
```

windows

``` bash
curl -X GET http://localhost:8002/get-db ^
     -H "Content-Type: application/json"
```

### Get songs from the all rings

ubuntu

``` bash
curl -X GET http://localhost:8001/get-songs 
```

### Get songs by key

ubuntu

``` bash
curl -X POST http://localhost:8001/get-song-by-key \
     -H "Content-Type: application/json" \
     -d "{\"key\": 1308545745728133123969321625266336910349324885359}"
```

### Get songs by title

ubuntu

``` bash
curl -X POST http://localhost:8001/get-songs-by-title \
     -H "Content-Type: application/json" \
     -d "{\"title\":\"t1\"}"
```

### Get songs by artist

ubuntu

``` bash
curl -X POST http://localhost:8001/get-songs-by-artist \
     -H "Content-Type: application/json" \
     -d "{\"artist\":\"a2\"}"
```

### Get songs by genre

ubuntu

``` bash
curl -X POST http://localhost:8001/get-songs-by-genre \
     -H "Content-Type: application/json" \
     -d "{\"genre\":\"g1\"}"
```