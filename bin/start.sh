#!/bin/bash
# Script will setup 

clear
#echo "Clearning earlier instances of watch"
#./kill_servers.sh
#echo "Setting up AJFS"
#./setup-ajfs-test.sh

file="file1"
fCounter=`cat fileCounter`
echo $(($fCounter + 1)) > fileCounter


while true; do
   for i in {1..2..1} #write to multiple clients
      do
        echo "Writing.." ./client_0$i/wf/file$((fCounter + 1))
        #echo "this is line one" >> ./client_0$i/wf/file$((fCounter + 1))
        echo $(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1) >> ./client_0$i/wf/file$((fCounter + 1))

        #Hardlink
        #sudo ln ./client_01/wf/file$((fCounter + 1)) ./client_02/wf/file$((fCounter + 1))

        #echo "this is line two" >> ./client_02/wf/file$((fCounter + 1))
      done
      sleep 5
done
exit 0
