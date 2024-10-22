#!/bin/bash

# Función para ejecutar un comando en una nueva terminal
run_in_new_terminal() {
    gnome-terminal --command="sudo -S bash -c 'echo \"$PASSWORD\" | docker run -it --name $1 -p $2:$2 -v .:/app spotify; exec bash'"
}

# Configurar la contraseña
read -s 3696

# Ejecutar los contenedores en terminales distintas
run_in_new_terminal first_container 8001 &
run_in_new_terminal second_container 8002 &
run_in_new_terminal third_container 8003 &
run_in_new_terminal fourth_container 8004 &
run_in_new_terminal fifth_container 8005 &

