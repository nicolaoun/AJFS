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

for i in {1..1..1} #write to multiple clients
   do
     echo ./client_01/wf/file$((fCounter + 1))
     #create backup file
     #touch ./client_01/wf/file$((fCounter + 1))
     echo "this is line one" >> ./client_01/wf/file$((fCounter + 1))

     #Hardlink
     #sudo ln ./client_01/wf/file$((fCounter + 1)) ./client_02/wf/file$((fCounter + 1))


     #echo ./client_02/wf/file$((fCounter + 1))
     #touch ./client_02/wf/file$((fCounter + 1))
     echo "this is line two" >> ./client_02/wf/file$((fCounter + 1))

   done
exit 0
