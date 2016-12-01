# Script will setup 
# 1. AJFS directory Structure within clients
# 2. copy watch, asm, and servers.list to each client
# 3. run servers outside client
# 4. run watch inside client
# Once changes are made to watched file, WRITE is invoked

clear
#echo "hello $USER"
#cal

echo "Creating file system..."

for i in {1..2..1} # start 2 clients
  do
     #Make directories
     mkdir ./client_0$i
     mkdir ./client_0$i/Journal
     mkdir ./client_0$i/temp_cjfs
     mkdir ./client_0$i/wf
     mkdir ./client_0$i/global
     mkdir ./client_0$i/receive

     #cp servers.list client_0$i/receive/servers.list
     #echo "Copied servers.list to client_0$i/receive/"

     python watch_cjfs.py 0$i &
     echo "Watching client_0$i"

  done

echo "Not Starting servers..."
#cd /home/seecs/Documents/AtomicSharedMemory/scripts
./run_server_ubuntu.sh 10 4000
#echo "Servers started..."

exit 0
