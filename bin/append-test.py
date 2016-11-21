#!/usr/bin/env python
import difflib
import os.path, time
import os
from random import randint
from datetime import datetime
import sys
import integrate



CURR_CLIENT_ = "01"
inodeid_ = 922294
received_jrnl = "client_"+CURR_CLIENT_+"/receive/A-"+str(inodeid_)
local_jrnl = "client_"+CURR_CLIENT_+"/Journal/A-"+str(inodeid_)

if os.path.isfile(received_jrnl):
   #use difflib here compare the two files
   difference = difflib.ndiff(open(local_jrnl).readlines(), open(received_jrnl).readlines())
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
   jrnl = open("client_"+CURR_CLIENT_+"/Journal/A-"+str(inodeid_), "a")
   for i in changes:
      jrnl.write(i+'\n')
   jrnl.close()

   lastWrittenToJrnl = integrate.get_global_record("client_"+CURR_CLIENT_+"/global/globalRecord",inodeid_,'get_last_line_written_to_jrnl')

   #update last line written to journal in global record
   #update_global_record(self, global_rec_file,inode_no,line_no,type_of_update)
   #update_global_record("client_"+CURR_CLIENT_+"/global/globalRecord",inodeid_,lastWrittenToJrnl+lineNo,'last_line_added')
