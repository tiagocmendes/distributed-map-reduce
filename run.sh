#!/bin/bash

gnome-terminal --title="Coordinator" -e "python3 src/coordinator.py -f src/lusiadas.txt"
gnome-terminal --title="Coordinator" -e "python3 src/coordinator.py -f src/lusiadas.txt"

sleep 3

for i in {1..4}
do
    gnome-terminal --title="Worker $i" -e "python3 src/worker.py" &
done