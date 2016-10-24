#!/bin/bash
# 1: # of servers
# 2: starting port
# 3: protocol

numservers=$1
numreaders=2
numwriters=2
port=$2
DIR=$(pwd)
foo=""

for ((i=0; i<$1; i=i+1))
do
    COMMAND=$(echo "$DIR/asm -t serve -p $port -i $i")
    #./term.sh -t $COMMAND

    #create new tab
    foo+=(--tab -e "$COMMAND")

    port=$(echo "$port+1" | bc)
done # servers

gnome-terminal "${foo[@]}"
