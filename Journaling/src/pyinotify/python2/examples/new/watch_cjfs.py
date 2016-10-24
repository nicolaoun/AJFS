# Notifier

import pyinotify
import time
import difflib
import shutil
import os.path
import os


wm = pyinotify.WatchManager()  # Watch Manager
#mask = pyinotify.IN_DELETE | pyinotify.IN_CREATE | pyinotify.IN_MODIFY | pyinotify.IN_MOVE_SELF | pyinotify.IN_ATTRIB # watched events

mask = pyinotify.ALL_EVENTS

class EventHandler(pyinotify.ProcessEvent):
    def process_IN_CREATE(self, event):
	#print event.pathname
	self.handle_file_modified(event.pathname, "Created-")	
	
    def process_IN_MOVED_TO(self, event):
	#print "Renamed-:", event.pathname, time.asctime( time.localtime(time.time()) )
	self.handle_file_modified(event.pathname, "Renamed-")

    def process_IN_MOVED_FROM(self, event):
	#print "Removing " + event.pathname + " from temp_cjfs"
	self.handle_file_modified(event.pathname, "Delete-Renamed-")



    def process_IN_DELETE(self, event):
        print "Deleted-: ", event.pathname, time.asctime( time.localtime(time.time()) )

    def process_IN_MODIFY(self, event):
	#print "Modified-:", event.pathname, time.asctime( time.localtime(time.time()) )
        self.handle_file_modified(event.pathname, "Modified-")
	
	
    def handle_file_modified(self, path, type_of_change):
	#print type_of_change,":", path, time.asctime( time.localtime(time.time()) )	
	splits = path.rsplit('/',2)
	fileNameCheckLen = len(splits[2])	
	checkGoutput = splits[2].rsplit('-',1)
	updatedFile = path #splits[0] + '/wf/' + splits[2].strip('~')
	bkupFile = splits[0] + '/temp_cjfs/' + splits[2].strip('~')
	
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
	      print type_of_change + ": " + path
	      #print splits[2][fileNameCheckLen-1]
	      #use difflib here compare the two files
	      difference = difflib.ndiff(open(bkupFile).readlines(), open(updatedFile).readlines())
	      jrnlWriteBuff = list(difference) #convert all changes to a list
	      print "After Diff: "
              print jrnlWriteBuff    
	      lineNo = 0
	      changes = "|"
              for i in jrnlWriteBuff:
                 print "Changes Before split: "+i
                 tempLine = i.split(' ')
                 print "Changes After split: "
                 print tempLine
		 if tempLine[0] == '+' or tempLine[0] == '-':
                    print str(lineNo) + chr(168) + tempLine[0] + chr(168) + tempLine[1].strip('\n') 
		    changes = changes + str(lineNo) + chr(168) + tempLine[0] + chr(168) + tempLine[1].strip('\n') + '|'
                 lineNo=lineNo+1

	      if not changes is "|":
	         #write changes in Journal
	         #gather inode info too
		 jrnl = open(splits[0] + "/Journal/A-" + str(inode_id), "a") 
	         jrnl.write(time.asctime( time.localtime(time.time()) ) + "," + splits[2].strip('~') + "," + str(inode_id) + "," + changes + '\n' )
	         jrnl.close
	         print "Appended-: Journal A-" + str(inode_id)
		 #once journal is written UPDATE file in temp_cjfs folder for next update
	         shutil.copy(updatedFile, bkupFile)
	         print "Updated-: Backup file " + bkupFile
	      else:
	         print "No changes to Journal and Backup"
	         
	   else:
	      print type_of_change + ": Not monitored file " + path
	            
	elif type_of_change == 'Created-' and checkGoutput[0] != '.goutputstream' and splits[2][fileNameCheckLen-1] != '~':
	      #shutil.copy(path, bkupFile)
	      open(bkupFile, 'a').close()
	      print "Created-: New file " + path
	      print "Created-: Backup Empty file " + bkupFile
	else: 
	      print "Nothing to do around here :("
	      #if checkGoutput[0] != '.goutputstream' and splits[2][fileNameCheckLen-1] != '~':
	      #   shutil.copy(path, bkupFile)
	      #   print "Backup file " + bkupFile + " updated"
	


handler = EventHandler()
notifier = pyinotify.Notifier(wm, handler)
wdd = wm.add_watch('/home/seecs/Documents/wf', mask, rec=True)

notifier.loop()
