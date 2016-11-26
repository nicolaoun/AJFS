#!/usr/bin/env python
import difflib
import os.path, time
import os
from random import randint
from datetime import datetime
import sys
import integrate


def update_global_record(global_rec_file,inode_no,line_no,type_of_update): #from watch_cjfs
	#print "inode: "+str(inode_no) + "line: " + str(line_no) + "type: " + type_of_update
	print("func:update_global_record()")
	print(inode_no)
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
		 #print tempgRFile[k]
	   #write back to globalRecord file
	   updategRec = open(global_rec_file,"w")
	   for l in range(0,len(tempgRFile)):
	      updategRec.write(tempgRFile[l] + '\n')
	   updategRec.close() 



CURR_CLIENT = "01"
inodeid_ = 922754
received_jrnl = "client_"+CURR_CLIENT+"/receive/A-"+str(inodeid_)
local_jrnl = "client_"+CURR_CLIENT+"/Journal/A-"+str(inodeid_)
globalRecordFile_ = "client_"+CURR_CLIENT+"/global/globalRecord"

print("func:appendMergeToLocalJrnl()")
print(inodeid_)

if os.path.isfile(received_jrnl):
    rcvd_jrnl = open(received_jrnl, "r") 
    lcl_jrnl = open(local_jrnl, "r")
    rcvd_ver = 3
    local_ver = int(integrate.get_global_record(globalRecordFile_,inodeid_,'get_jrnl_ver'))
    
    if (local_ver < rcvd_ver):  
        #use difflib here compare the two files
        #difference = difflib.ndiff(open(local_jrnl).readlines(), open(received_jrnl).readlines())
        difference = difflib.ndiff(lcl_jrnl.readlines(), rcvd_jrnl.readlines())
        jrnlChanges = list(difference) #convert all changes to a list
        #print jrnlChanges
        lineNo = 0
        changes = []
        for i in jrnlChanges:
           #print i
           tempLine = i.split('\n')
           if tempLine[0][:1] == '+' or tempLine[0][:1] == '-':
	       changes.append(tempLine[0][2:].strip('\n'))
	       lineNo = lineNo + 1 
        print changes
        print lineNo
   
   
        #Append local journal with new lines
        jrnl = open("client_"+CURR_CLIENT+"/Journal/A-"+str(inodeid_), "a")
        for i in changes:
            jrnl.write(i+'\n')
        jrnl.close()

        lastWrittenToJrnl = integrate.get_global_record(globalRecordFile_,inodeid_,'get_last_line_written_to_jrnl')
        print "get last line written: " + lastWrittenToJrnl
        newLastWrittenToJrnl = int(lastWrittenToJrnl) + lineNo + 1
        print "new last line: " + str(newLastWrittenToJrnl)
        #update last line written to journal in global record
        #update_global_record(self, global_rec_file,inode_no,line_no,type_of_update)
        update_global_record(globalRecordFile_,inodeid_,newLastWrittenToJrnl,'last_line_added')
	lcl_jrnl.close()
	rcvd_jrnl.close()

	integrate.integrate_jrnl(CURR_CLIENT,inodeid_,'local')
    else:
	print("Received Journal Discarded..." + "l:" + str(local_ver) + "|r:" + str(rcvd_ver))
else:
    print("Journal not received")

