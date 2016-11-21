#!/usr/bin/env python
import difflib
import os.path, time
import os
from random import randint
from datetime import datetime
import sys



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
   changes = ""
   for i in jrnlChanges:
      #print i
      tempLine = i.split('\n')
      if tempLine[0][:1] == '+' or tempLine[0][:1] == '-':
         changes = changes + i.strip('\n') + "\n"
   print changes
         
	#update last line written to journal in global record
