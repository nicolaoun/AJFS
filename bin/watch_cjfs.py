#!/usr/bin/env python

import pyinotify
import time
import difflib
import shutil
import os.path, time
import os
from random import randint
from datetime import datetime
import sys
import subprocess
import integrate
import json

wm = pyinotify.WatchManager()  # Watch Manager
#mask = pyinotify.IN_DELETE | pyinotify.IN_CREATE | pyinotify.IN_MODIFY | pyinotify.IN_MOVE_SELF | pyinotify.IN_ATTRIB # watched events

mask = pyinotify.ALL_EVENTS
JOURNAL_SIZE = 30
CURR_CLIENT = "0"

class EventHandler(pyinotify.ProcessEvent):
    global CURR_CLIENT
    #def __init__(self, currentClient):
    #    self.CURRENT_CLIENT = currentClient

    def process_IN_CREATE(self, event):
	self.handle_file_modified(event.pathname, "Created-")	
	
    def process_IN_MOVED_TO(self, event):
	self.handle_file_modified(event.pathname, "Renamed-")

    def process_IN_MOVED_FROM(self, event):
	self.handle_file_modified(event.pathname, "Delete-Renamed-")



    def process_IN_DELETE(self, event):
       	splits = event.pathname.rsplit('/',2)	
	deletedFileFromTemp = splits[0] + '/temp_cjfs/' + splits[2].strip('~')
	if deletedFileFromTemp[-4:] != '.swp':
		os.remove(deletedFileFromTemp)
		#print "Deleted-: ", event.pathname, str(datetime.now())#time.asctime( time.localtime(time.time()) )
		#print "Deleted-: ", deletedFileFromTemp, str(datetime.now())#time.asctime( time.localtime(time.time()) )
		openGlobalRec = open(splits[0] + '/global/globalRecord', "r")
	 	line = openGlobalRec.readline()
		while line!="": #increment global rec lines
		   globalSplit = line.split(',')
		   if globalSplit[0] == event.pathname:
		      shutil.move(splits[0]+'/Journal/A-'+globalSplit[1].strip('\n'), splits[0]+'/Journal/A-'+globalSplit[1].strip('\n')+'-'+str(randint(1,10000))+'-deleted')
		      print "Marked: Journal A-" + globalSplit[1].strip('\n') + " as deleted"
	 	   line = openGlobalRec.readline()


    def process_IN_MODIFY(self, event):
        self.handle_file_modified(event.pathname, "Modified-")
	
	
	
    def update_global_record(self, global_rec_file,inode_no,line_no,type_of_update):
	print("func:update_global_record()")
	print(inode_no)
        if 1==1:
           tempgRFile = []
           with open(global_rec_file) as fil:
              tempgRFile = fil.read().splitlines()
           
           for k in range(0,len(tempgRFile)): #update list
              globalSplit = tempgRFile[k].split(',')
              if globalSplit[1].strip('\n') == str(inode_no):
		 if type_of_update == 2: #2=last_line_added
                    globalSplit[2] = str(line_no-1)
                    globalSplit[3] = globalSplit[3].strip('\n')
                 elif type_of_update == 3 and int(globalSplit[3].strip('\n')) > 0: #3=last_line_added_post_rollup
                    globalSplit[2] = globalSplit[2].strip('\n')
                    globalSplit[3] = str(int(globalSplit[3].strip('\n'))-1)
                    globalSplit[4]= str(int(globalSplit[4])+1)
                 elif type_of_update == 3 and int(globalSplit[3].strip('\n')) < 0:
		    globalSplit[2] = globalSplit[2].strip('\n')
                    globalSplit[4]= str(int(globalSplit[4])+1)
                 tempgRFile[k] = globalSplit[0]+','+globalSplit[1]+','+globalSplit[2]+','+globalSplit[3]+','+globalSplit[4]
           updategRec = open(global_rec_file,"w") #write back to globalRecord file
           for l in range(0,len(tempgRFile)):
              updategRec.write(tempgRFile[l] + '\n')
           updategRec.close() 

    def write_to_journal(self,jrnl_path,file_name, changes_, inodeid, globalRecordFile):
	print("func:write_to_journal()")
	print(file_name + " " + str(inodeid))
        global CURR_CLIENT
	lineToBeAdded = str(datetime.now()) + ',' + str(file_name) + ',' + str(inodeid) + ',' + changes_
	tempJrnl = [] #Journal will be temporarily loaded into memory to implement line limit (threshold)
	if os.path.isfile(jrnl_path):
	   with open(jrnl_path) as fil:
       	       tempJrnl = fil.read().splitlines()
	
	   if len(tempJrnl) >= JOURNAL_SIZE:
	      print "Journal Threshold Reached...Deleting from Top..."
	      del tempJrnl[0]
	      tempJrnl.append(lineToBeAdded )
              self.update_global_record(globalRecordFile,inodeid,0,3)
	   else:
              tempJrnl.append(lineToBeAdded)
	   
	   jrnl = open(jrnl_path, "w")
	   for i in tempJrnl:
	      jrnl.write(i+'\n')
	   jrnl.close()

	   #WRITE
	
	else: #if the journal does not exist
           print "Creating new journal..."
	   jrnl = open(jrnl_path, "a")			
	   jrnl.write(lineToBeAdded)
	   jrnl.close() 
	   
    #MAKE PERIODIC
    def READ(self, inodeid_):
	print("func:READ()")
        #Read Journal from Shared Memory
	global CURR_CLIENT
        subprocess.call(["./asm", "-t read", "-i "+CURR_CLIENT+" ", "-o A-"+str(inodeid_)+" ","-f client_"+CURR_CLIENT+"/receive"])
	globalRecordFile_ = "client_" + CURR_CLIENT + "/global/globalRecord" 
	#Compare version, if rcvd version greater
	#new journal was written to shared memory (are we outdated for circular journal)
	#issue a fetch command (return file(for circular), journal and meta data) 
	#OVERWITE LOCAL JOURNAL

	appendMergeToLocalJrnl(self,inodeid_,globalRecordFile_)
        integrate.integrate_jrnl(CURR_CLIENT,inodeid_,'local')
	WRITE(inodeid_)
	
    def WRITE(self, inodeid_):
	print("func:WRITE()")
	#WRITE TO SHARED MEMORY
        global CURR_CLIENT
        #subprocess.call(["./asm", "-t write", "-i "+CURR_CLIENT+" ", "-o A-"+str(inodeid)+" ","-f client_"+CURR_CLIENT+"/Journal"])
        
        #PUT IN WHILE LOOP
        subprocess.call(["cp", "client_"+CURR_CLIENT+"/Journal/A-"+str(inodeid_), "client_"+CURR_CLIENT+"/receive/A-"+str(inodeid_)])
	process = subprocess.Popen(["./asm", "-d 3" ,"-t write", "-i "+CURR_CLIENT+" ", "-o A-"+str(inodeid_)+" ","-f client_"+CURR_CLIENT+"/receive"],stdout=subprocess.PIPE)
	success, err = process.communicate()
	print "WRITE RETURNED: "
	print success, err		
	#success = 0 #FIX THIS and JOURNAL NOT RECEIVED IN receive FOLDER AT THE CLIENT
	if success == 1:
	   #integrate.merge_journals(CURR_CLIENT, inodeid)
	   #READ(inodeid_)
	   appendMergeToLocalJrnl(inodeid_,"client_"+CUR_CLIENT+"/global/globalRecord")
	
        #write local journal to shared memory
        subprocess.call(["cp", "client_"+CURR_CLIENT+"/Journal/A-"+str(inodeid_), "client_"+CURR_CLIENT+"/receiveA-"+str(inodeid_)])
        process = subprocess.Popen(["./asm", "-d 3","-t write", "-i "+CURR_CLIENT+" ", "-o A-"+str(inodeid_)+" ","-f client_"+CURR_CLIENT+"/receive"],stdout=subprocess.PIPE)
	success, err = process.communicate()
	print "POST MERGE WRITE RETURNED: "
	print success, err

    def getReceivdeRecord(self,inodeid_,serverID_,reqType):
	#rcvdMetaData = open("server_"+serverID_+"/.meta/A-"+inodeid_+".meta"", "r")
	metaPath ="server_"+serverID_+"/.meta/A-"+inodeid_+".meta"

	with open(metaPath) as json_data:
	   d = json.load(json_data)
	
	if (reqType == "get_RJV"):
	   return d["ts"]
	elif (reqType == "get_LW"):
	   return d["wid"]


    def appendMergeToLocalJrnl(self,inodeid_,globalRecordFile_):
	received_jrnl = "client_"+CURR_CLIENT+"/receive/A-"+str(inodeid_)
	local_jrnl = "client_"+CURR_CLIENT+"/Journal/A-"+str(inodeid_)
	globalRecordFile_ = "client_"+CURR_CLIENT+"/global/globalRecord"

	print("func:appendMergeToLocalJrnl()")
	print(inodeid_)

	if os.path.isfile(received_jrnl):
	    rcvd_jrnl = open(received_jrnl, "r") 
	    lcl_jrnl = open(local_jrnl, "r")
	    rcvd_ver = self.getReceivedRecord(inodeid_,1,"get_RJV") #get rcvd jrnl ver
	    rcvd_ver_last_writer = self.getTeceivedRecord(inodeid_,1,"get_LW") #get rcvd jrnl last writer
	    local_ver = int(integrate.get_global_record(globalRecordFile_,inodeid_,'get_jrnl_ver'))
	    
	    if (local_ver < rcvd_ver):
	      if (rcvd_ver_last_writer != int(CURR_CLIENT)):  
	         difference = difflib.ndiff(lcl_jrnl.readlines(), rcvd_jrnl.readlines())
		 jrnlChanges = list(difference) #convert all changes to a list
		 lineNo = 0
		 changes = []
		 for i in jrnlChanges:
		    tempLine = i.split('\n')
		    if tempLine[0][:1] == '+' or tempLine[0][:1] == '-':
		        changes.append(tempLine[0][2:].strip('\n'))
		        lineNo = lineNo + 1 
		 print changes
		 print lineNo
	   
		 jrnl = open("client_"+CURR_CLIENT+"/Journal/A-"+str(inodeid_), "a") #Append local journal with new lines
		 for i in changes:
		     jrnl.write(i+'\n')
		 jrnl.close()

		 lastWrittenToJrnl = integrate.get_global_record(globalRecordFile_,inodeid_,'get_last_line_written_to_jrnl')
		 print "get last line written: " + lastWrittenToJrnl
		 newLastWrittenToJrnl = int(lastWrittenToJrnl) + lineNo + 1
		 print "new last line: " + str(newLastWrittenToJrnl)
		 update_global_record(globalRecordFile_,inodeid_,newLastWrittenToJrnl,2)
		 lcl_jrnl.close()
		 rcvd_jrnl.close()
	    else:
		 print("Received Journal Discarded..." + "l:" + str(local_ver) + "|r:" + str(rcvd_ver))
	else:
	    print("Journal not received")


    def handle_file_modified(self, path, type_of_change):
	print("func:handle_file_modified()")
	print(path)
	splits = path.rsplit('/',2)
	fileNameCheckLen = len(splits[2])	
	checkGoutput = splits[2].rsplit('-',1)
	updatedFile = path 
	bkupFile = splits[0] + '/temp_cjfs/' + splits[2].strip('~')
        globalRecFile = splits[0] + '/global/globalRecord' # to keep inode filename mapping
	
	tempF = os.open(updatedFile, os.O_RDWR|os.O_CREAT)
	info = os.fstat(tempF) 
	inode_id = info.st_ino #find inode_id for the created file
	if (type_of_change == 'Delete-Renamed-' and
           checkGoutput[0][-4:] != '.swp' and
 	   checkGoutput[0] != '.goutputstream' and
           splits[2][fileNameCheckLen-1] != '~'):
           print("-->Delete-Renamed-")
	   if os.path.isfile(bkupFile):
	      os.remove(bkupFile)
	      print ("Removed-:" + bkupFile)
	   else:
              print("File not found: " + bkupFile)	
	
	elif (type_of_change == 'Modified-' and 
	     checkGoutput[0] != '.goutputstream' and 
             checkGoutput[0][-4:] != '.swp' and 
             splits[2][fileNameCheckLen-1] != '~'):
           print("-->Modified-")
	   if os.path.isfile(bkupFile):
	      difference = difflib.ndiff(open(bkupFile).readlines(), open(updatedFile).readlines())
	      jrnlWriteBuff = list(difference) #convert all changes to a list
	      lineNo = 0
	      changes = "|"
              for i in jrnlWriteBuff:
                 tempLine = i.split(' ')
                 change_transac = ""
                 for chunks in range(1,len(tempLine)): # find a better way to do this
                    change_transac += tempLine[chunks] + " "
		 change_transac = change_transac[:-1]
		 if tempLine[0] == '+' or tempLine[0] == '-':
		    changes = changes + str(lineNo) + chr(168) + tempLine[0] + chr(168) + change_transac.strip('\n') + '|'
                 lineNo=lineNo+1 #This line no is not going to be written on the Global Record


	      print changes	 
	      
	      if not changes is "|":
		 jrnlPath = splits[0] + "/Journal/A-" + str(inode_id)
	         self.write_to_journal(jrnlPath,splits[2].strip('~'),changes,inode_id,globalRecFile) #write changes to journal
	         shutil.copy(updatedFile, bkupFile) #once journal is written UPDATE file in temp_cjfs folder for next update
   		 noOfLinesInJournal = sum(1 for line in open(jrnlPath))
	         self.update_global_record(globalRecFile,inode_id,noOfLinesInJournal,2)

                 #self.appendMergeToLocalJrnl(inode_id,globalRecFile)
		 sTime = time.time()
		 self.WRITE(inode_id)
                 tTime = time.time() - sTime
		 recordTime = open('client_'+CURR_CLIENT+'/ttime/write-times.txt' ,'a')
		 recordTime.write(str(inode_id)+ ',' +str(tTime)+"\n")
                 recordTime.close()

	      else:
	         print "No changes to Journal or Backup"
	         
	   else:
	      print type_of_change + ": Not monitored file " + path
	            
	elif (type_of_change == 'Created-' and
             checkGoutput[0] != '.goutputstream' and
             checkGoutput[0][-4:] != '.swp' and
             splits[2][fileNameCheckLen-1] != '~'):
           print("-->Created-")
	   open(bkupFile, 'a').close()
           globalRec = open(globalRecFile,"a")
	   #0,last line added to journal, -1,last line written to file, 0,rolled up lines
	   globalRec.write(path+','+ str(inode_id) + ',0,-1,0\n') #create first global record entry
           globalRec.close()
	else: 
	   print "Unhandled event..."


def main():
    for arg in sys.argv[1:]:
        print arg
        global CURR_CLIENT
        CURR_CLIENT = arg
        handler = EventHandler()
        notifier = pyinotify.Notifier(wm, handler)
        wdd = wm.add_watch('client_'+CURR_CLIENT+'/wf', mask, rec=True)
        notifier.loop()

if __name__ == "__main__":
    main()
