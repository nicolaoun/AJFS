# Notifier

import pyinotify
#import time
import difflib
import shutil
import os.path
import os
from random import randint
from datetime import datetime
import sys
import subprocess


wm = pyinotify.WatchManager()  # Watch Manager
#mask = pyinotify.IN_DELETE | pyinotify.IN_CREATE | pyinotify.IN_MODIFY | pyinotify.IN_MOVE_SELF | pyinotify.IN_ATTRIB # watched events

mask = pyinotify.ALL_EVENTS
JOURNAL_SIZE = 10
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
	os.remove(deletedFileFromTemp)
        print "Deleted-: ", event.pathname, str(datetime.now())#time.asctime( time.localtime(time.time()) )
        print "Deleted-: ", deletedFileFromTemp, str(datetime.now())#time.asctime( time.localtime(time.time()) )
	openGlobalRec = open(splits[0] + '/global/globalRecord', "r")
 	line = openGlobalRec.readline()
        while line!="": #increment global rec lines
           #print "NEW LINE"
           #print line 
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
	if type_of_change == 'Delete-Renamed-' and checkGoutput[0] != '.goutputstream':
	   if os.path.isfile(bkupFile):
	      os.remove(bkupFile)
	      print ("Removed-:" + bkupFile)	

	#elif splits[2][fileNameCheckLen-1] == '~' or type_of_change == 'Modified-':
	elif type_of_change == 'Modified-' and checkGoutput[0] != '.goutputstream' and splits[2][fileNameCheckLen-1] != '~':
	   if os.path.isfile(bkupFile):
	      #print type_of_change + ": " + path
	      #print splits[2][fileNameCheckLen-1]
	      #use difflib here compare the two files
	      difference = difflib.ndiff(open(bkupFile).readlines(), open(updatedFile).readlines())
	      jrnlWriteBuff = list(difference) #convert all changes to a list
	      #print "After Diff: "
              #print jrnlWriteBuff    
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
	            
	elif type_of_change == 'Created-' and checkGoutput[0] != '.goutputstream' and splits[2][fileNameCheckLen-1] != '~':
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
           
           #Write journal to Shared Memory
           global CURR_CLIENT
           #fetch_jrnl_path = "./asm -t write -i "+CURR_CLIENT+" -o A-"+str(inodeid)+" -f ~/Documents/NEW-AJFS/client_"+CURR_CLIENT+"/Journal"
           fetch_jrnl_path = "./asm -t write -i "+CURR_CLIENT+" -o A-"+str(inodeid)+" -f client_"+CURR_CLIENT+"/Journal" 
           print fetch_jrnl_path
           subprocess.call(fetch_jrnl_path)
   
	else: #if the journal does not exist
	   jrnl = open(jrnl_path, "a")
	   jrnl.write(lineToBeAdded)
	   jrnl.close() 	


def main():
    for arg in sys.argv[1:]:
        print arg
        global CURR_CLIENT
        CURR_CLIENT = arg
        handler = EventHandler()
        notifier = pyinotify.Notifier(wm, handler)
        #wdd = wm.add_watch('/home/seecs/Documents/NEW-AJFS/client_'+CURR_CLIENT+'/wf', mask, rec=True)
        wdd = wm.add_watch('client_'+CURR_CLIENT+'/wf', mask, rec=True)
        notifier.loop()

if __name__ == "__main__":
    main()
