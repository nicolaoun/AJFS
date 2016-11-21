# Notifier

import pyinotify
#import time
import difflib
import shutil
import os.path, time
import os
from random import randint
from datetime import datetime
import sys
import subprocess
import integrate


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
	#print event.pathname
	self.handle_file_modified(event.pathname, "Created-")	
	
    def process_IN_MOVED_TO(self, event):
	self.handle_file_modified(event.pathname, "Renamed-")

    def process_IN_MOVED_FROM(self, event):
	#print "Removing " + event.pathname + " from temp_cjfs"
	self.handle_file_modified(event.pathname, "Delete-Renamed-")



    def process_IN_DELETE(self, event):
       	splits = event.pathname.rsplit('/',2)	
	deletedFileFromTemp = splits[0] + '/temp_cjfs/' + splits[2].strip('~')
	if deletedFileFromTemp[-4:] != '.swp':
		os.remove(deletedFileFromTemp)
		print "Deleted-: ", event.pathname, str(datetime.now())#time.asctime( time.localtime(time.time()) )
		print "Deleted-: ", deletedFileFromTemp, str(datetime.now())#time.asctime( time.localtime(time.time()) )
		openGlobalRec = open(splits[0] + '/global/globalRecord', "r")
	 	line = openGlobalRec.readline()
		while line!="": #increment global rec lines
		   globalSplit = line.split(',')
		   #print event.pathname
		   #print globalSplit[0]
		   if globalSplit[0] == event.pathname:
		      shutil.move(splits[0]+'/Journal/A-'+globalSplit[1].strip('\n'), splits[0]+'/Journal/A-'+globalSplit[1].strip('\n')+'-'+str(randint(1,10000))+'-deleted')
		      print "Marked: Journal A-" + globalSplit[1].strip('\n') + " as deleted"
	 	   line = openGlobalRec.readline()


    def process_IN_MODIFY(self, event):
        self.handle_file_modified(event.pathname, "Modified-")
	
	
    def handle_file_modified(self, path, type_of_change):
	splits = path.rsplit('/',2)
	fileNameCheckLen = len(splits[2])	
	checkGoutput = splits[2].rsplit('-',1)
	updatedFile = path #splits[0] + '/wf/' + splits[2].strip('~')
	bkupFile = splits[0] + '/temp_cjfs/' + splits[2].strip('~')
        globalRecFile = splits[0] + '/global/globalRecord' # to keep inode filename mapping
	
	#find inode_id for the created file
	tempF = os.open(updatedFile, os.O_RDWR|os.O_CREAT)
	info = os.fstat(tempF) 
	inode_id = info.st_ino
	#print inode_id
	
	#print checkGoutput
	if checkGoutput[0][-4:] == '.swp' or  checkGoutput[0] == '.goutputstream': # and splits[2][fileNameCheckLen-1] != '~':
	   print("-")

	#elif type_of_change == 'Delete-Renamed-' and checkGoutput[-4:] == '.swp' or  checkGoutput[0] != '.goutputstream':
	elif type_of_change == 'Delete-Renamed-':
	   if os.path.isfile(bkupFile):
	      os.remove(bkupFile)
	      print ("Removed-:" + bkupFile)	
	
	#elif type_of_change == 'Modified-' and checkGoutput[0] != '.goutputstream' or checkGoutput[-4:] != '.swp' and splits[2][fileNameCheckLen-1] != '~':
	elif type_of_change == 'Modified-':
	   if os.path.isfile(bkupFile):
	      #use difflib here compare the two files
	      difference = difflib.ndiff(open(bkupFile).readlines(), open(updatedFile).readlines())
	      jrnlWriteBuff = list(difference) #convert all changes to a list
	      lineNo = 0
	      changes = "|"
              for i in jrnlWriteBuff:
                 #print "Changes Before split: "+i
                 tempLine = i.split(' ')
                 #print "Changes After split: "
                 #print tempLine
                 change_transac = ""
                 for chunks in range(1,len(tempLine)): # find a better way to do this
                    change_transac += tempLine[chunks] + " "
		 change_transac = change_transac[:-1]
	  	 #print change_transac
		 if tempLine[0] == '+' or tempLine[0] == '-':
		    changes = changes + str(lineNo) + chr(168) + tempLine[0] + chr(168) + change_transac.strip('\n') + '|'
                 lineNo=lineNo+1 #This line no is not going to be written on the Global Record


	      print changes	 
	      
	      if not changes is "|":
	         #write changes in Journal
	         #gather inode info too
		 jrnlPath = splits[0] + "/Journal/A-" + str(inode_id)
		 #write changes to journal
	         self.write_to_journal(jrnlPath,splits[2].strip('~'),changes,inode_id,globalRecFile)
	         print "Appended-: " + jrnlPath 
		 #once journal is written UPDATE file in temp_cjfs folder for next update
	         shutil.copy(updatedFile, bkupFile)
	         print "Updated-: Backup file " + bkupFile
		 #Find current Journal size
   		 noOfLinesInJournal = sum(1 for line in open(jrnlPath))
	         #update last added Journal line counter to globalRecordFile
	         #path,inode_id,last_line_added_to_journal,last_line_written_to_file
	         self.update_global_record(globalRecFile,inode_id,noOfLinesInJournal,'last_line_added')

	      else:
	         print "No changes to Journal or Backup"
	         
	   else:
	      print type_of_change + ": Not monitored file " + path
	            
	#elif type_of_change == 'Created-' and checkGoutput[0] != '.goutputstream' or checkGoutput[-4:] != '.swp' and splits[2][fileNameCheckLen-1] != '~':
	elif type_of_change == 'Created-':
	      #shutil.copy(path, bkupFile)
	      open(bkupFile, 'a').close()
	      print "Created-: New file " + path
	      print "Created-: Backup Empty file " + bkupFile
              globalRec = open(globalRecFile,"a")
	      globalRec.write(path+','+ str(inode_id) + ',0,-1,0\n')
              globalRec.close()
      	      print "Global Record Updated..."
	else: 
	      print "Unhandled event..."
	      #if checkGoutput[0] != '.goutputstream' and splits[2][fileNameCheckLen-1] != '~':
	      #   shutil.copy(path, bkupFile)
	      #   print "Backup file " + bkupFile + " updated"
	
    def update_global_record(self, global_rec_file,inode_no,line_no,type_of_update):
	print "inode: "+str(inode_no) + "line: " + str(line_no) + "type: " + type_of_update
        if 1==1:#type_of_update == 'last_line_added':
           tempgRFile = []
           with open(global_rec_file) as fil:
              tempgRFile = fil.read().splitlines()
           #update list
           for k in range(0,len(tempgRFile)):
              globalSplit = tempgRFile[k].split(',')
	      #print globalSplit
              if globalSplit[1].strip('\n') == str(inode_no):
		 #print "i'm here"
		 if type_of_update == 'last_line_added':
                    globalSplit[2] = str(line_no-1)
                    globalSplit[3] = globalSplit[3].strip('\n')
                    #globalSplit[4]= str(0)
                 elif type_of_update == 'last_line_added_post_rollup' and int(globalSplit[3].strip('\n')) > 0:
                    globalSplit[2] = globalSplit[2].strip('\n')
                    globalSplit[3] = str(int(globalSplit[3].strip('\n'))-1)
                    globalSplit[4]= str(int(globalSplit[4])+1)
                 elif type_of_update == 'last_line_added_post_rollup' and int(globalSplit[3].strip('\n')) < 0:
		    globalSplit[2] = globalSplit[2].strip('\n')
                    #globalSplit[3] = str(int(globalSplit[3].strip('\n'))-1)
                    globalSplit[4]= str(int(globalSplit[4])+1)
                 tempgRFile[k] = globalSplit[0]+','+globalSplit[1]+','+globalSplit[2]+','+globalSplit[3]+','+globalSplit[4]
		 #print tempgFile[k]
           #write back to globalRecord file
           updategRec = open(global_rec_file,"w")
           for l in range(0,len(tempgRFile)):
              updategRec.write(tempgRFile[l] + '\n')
           updategRec.close() 

    def write_to_journal(self,jrnl_path,file_name, changes_, inodeid, globalRecordFile):
        global CURR_CLIENT
	lineToBeAdded = str(datetime.now()) + ',' + str(file_name) + ',' + str(inodeid) + ',' + changes_
	tempJrnl = [] #Journal will be temporarily loaded into memory
		      #to implement line limit (threshold)
	if os.path.isfile(jrnl_path):
	   with open(jrnl_path) as fil:
       	       tempJrnl = fil.read().splitlines()

	   #for i in range(0,len(tempJrnl)):
	
	   if len(tempJrnl) >= JOURNAL_SIZE:
	      print "Journal Threshold Reached...Deleting from Top..."
	      del tempJrnl[0]
	      tempJrnl.append(lineToBeAdded )
              self.update_global_record(globalRecordFile,inodeid,0,'last_line_added_post_rollup')
	   else:
              tempJrnl.append(lineToBeAdded)
	   #print tempJrnl #write this to a new journal file?
	   
	   jrnl = open(jrnl_path, "w")
	   for i in tempJrnl:
	      jrnl.write(i+'\n')
	   jrnl.close()

	   """
           #Write journal to Shared Memory
	   #Before writing to shared memory fetch the latest ver from shared storage
           subprocess.call(["./asm", "-t read", "-i "+CURR_CLIENT+" ", "-o A-"+str(inodeid)+" ","-f client_"+CURR_CLIENT+"/receive"])     
           if os.path.isfile("client_"+CURR_CLIENT+"/receive/A-"+str(inodeid)):
              received_jrnl_time_stamp = os.path.getmtime("client_"+CURR_CLIENT+"/receive/A-"+str(inodeid))
              print "RECEIVED JRNL: "
              print received_jrnl_time_stamp
              local_jrnl_time_stamp = os.path.getmtime("client_"+CURR_CLIENT+"/Journal/A-"+str(inodeid))
              print "LOCAL JRNL: "
              print local_jrnl_time_stamp
      
	   #compare the version with the version that we have locally at the client
           #if received version newer
           if os.path.isfile("client_"+CURR_CLIENT+"/receive/A-"+str(inodeid)):
              
              if ( received_jrnl_time_stamp > local_jrnl_time_stamp):
                 print "Integrating..."
                 #integrate to client_0x/wf
                 integrate.integrate_jrnl(CURR_CLIENT, inodeid)             
                 #generate new lines and append to local journal client_0x/Journal
                 #write to shared storage
                 #subprocess.call(["./asm", "-t write", "-i "+CURR_CLIENT+" ", "-o A-"+str(inodeid)+" ","-f client_"+CURR_CLIENT+"/Journal"])
              else:
                 #write to shared storage
                 print "Writing to shared storage...(w/o integrating)"
                 subprocess.call(["./asm", "-t write", "-i "+CURR_CLIENT+" ", "-o A-"+str(inodeid)+" ","-f client_"+CURR_CLIENT+"/Journal"])
              
           else:
              print "No received Journal to compare at: client_"+CURR_CLIENT+"  A-"+str(inodeid)

           """

	else: #if the journal does not exist
           print "Journal does not exist. Creating..."# and writing to shared storage..."
	   jrnl = open(jrnl_path, "a")			
	   jrnl.write(lineToBeAdded)
	   jrnl.close() 

"""
WRITE TO SHARED MEMORY
           #Write journal to Shared Memory
           #global CURR_CLIENT
           #subprocess.call(["./asm", "-t write", "-i "+CURR_CLIENT+" ", "-o A-"+str(inodeid)+" ","-f client_"+CURR_CLIENT+"/Journal"])
        
        #PUT IN WHILE LOOP
        subprocess.call(["cp", "client_"+CURR_CLIENT+"/Journal/A-"+str(inodeid), "client_"+CURR_CLIENT+"/receive/A-"+str(inodeid)])
	process = subprocess.Popen(["./asm", "-d 3" ,"-t write", "-i "+CURR_CLIENT+" ", "-o A-"+str(inodeid)+" ","-f client_"+CURR_CLIENT+"/receive"],stdout=subprocess.PIPE)
	success, err = process.communicate()
	print "WRITE RETURNED: "
	print success, err		
	#success = 0 #FIX THIS and JOURNAL NOT RECEIVED IN receive FOLDER AT THE CLIENT
	if success == 1:
	   integrate.merge_journals(CURR_CLIENT, inodeid)
	
        #write local journal to shared memory
        subprocess.call(["cp", "client_"+CURR_CLIENT+"/Journal/A-"+str(inodeid), "client_"+CURR_CLIENT+"/receiveA-"+str(inodeid)])
        process = subprocess.Popen(["./asm", "-d 3","-t write", "-i "+CURR_CLIENT+" ", "-o A-"+str(inodeid)+" ","-f client_"+CURR_CLIENT+"/receive"],stdout=subprocess.PIPE)
	success, err = process.communicate()
	print "POST MERGE WRITE RETURNED: "
	print success, err
"""

    def appendToLocalJrnl(CURR_CLIENT_, inodeid_ ): #append new lines from recieved journal to local journal
	received_jrnl = "client_"+CURR_CLIENT_+"/receive/A-"+str(inodeid_)
	local_jrnl = "client_"+CURR_CLIENT_+"/Journal/A-"+str(inodeid_)
	if os.path.isfile(received_jrnl):
	      #use difflib here compare the two files
	      difference = difflib.ndiff(open(received_jrnl).readlines(), open(local_jrnl).readlines())
	      jrnlChanges = list(difference) #convert all changes to a list
	      lineNo = 0
	      changes = "|"
              for i in jrnlChanges:
                 print "Changes Before split: "+i
                 tempLine = i.split(' ')
                 print "Changes After split: "
                 print tempLine
                 """
		 change_transac = ""
                 for chunks in range(1,len(tempLine)): # find a better way to do this
                    change_transac += tempLine[chunks] + " "
		 change_transac = change_transac[:-1]
	  	 #print change_transac
		 if tempLine[0] == '+' or tempLine[0] == '-':
		    changes = changes + str(lineNo) + chr(168) + tempLine[0] + chr(168) + change_transac.strip('\n') + '|'
                 lineNo=lineNo+1 #This line no is not going to be written on the Global Record
		 """


def main():
    for arg in sys.argv[1:]:
        print arg
        global CURR_CLIENT
        CURR_CLIENT = arg
        handler = EventHandler()
        notifier = pyinotify.Notifier(wm, handler)
        #wdd = wm.add_watch('/home/seecs/Documents/NEW-AJFS/client_'+CURR_CLIENT+'/wf', mask, rec=True)
        wdd = wm.add_watch('client_'+CURR_CLIENT+'/wf', mask, rec=True)
        #wdd1 = wm.add_watch('client_'+CURR_CLIENT+'/receive', mask, rec=False)
        notifier.loop()

if __name__ == "__main__":
    main()
