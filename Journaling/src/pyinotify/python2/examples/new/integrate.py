#!/usr/bin/env python
import shutil
import os
import time

#--- set the time interval to check for new files (in seconds) below 
#    this interval should be smaller than the interval new files appear!
t = 1
#--- set the source directory (dr1) and target directory (dr2)
dr1 = "/home/seecs/Documents/Journal"
dr2 = "/home/seecs/Documents/AJFS"

#name = "output_"; extension = ".txt"
#newname = lambda n: dr2+"/"+name+str(n)+extension

#while True:
reconstFileText = {}
newfiles = os.listdir(dr1)
for file in newfiles:
    print "Found Journal: "+file
    if file == "A-915424":
       j=open(dr1+'/'+file,'r')
       currentJrnlLine=0
       line = j.readline()
       while line!="": #increment journal lines
          firstSplit = line.split(',') # split record
          #print firstSplit
          secondSplit = firstSplit[3].split('|') # split changes
          secondSplit.remove('\n') # handle some formating issues
          secondSplit.remove('')
          #print secondSplit
          decrementFlag=0 #to keep track of multiple removes in a single transaction
          for change in secondSplit: #implement each change individually
             parameter = change.split(' ')#chr(168)) # change parameters
	     if parameter[1] == '+' and currentJrnlLine == 0:
	        reconstFileText[int(parameter[0])]=parameter[2] #write first line as it is from jrnl
	     elif parameter[1] == '+' and currentJrnlLine >= 1: #accomodate +'s
                for i in range (len(reconstFileText),int(parameter[0]),-1):
	     	   reconstFileText[i] = reconstFileText[i-1]
                   print str(i-1)+"into"+str(i)
                reconstFileText[int(parameter[0])]=parameter[2]    
                print "Added Line: "+str(int(parameter[0]))
                print reconstFileText
             elif parameter[1] == '-':
                #print reconstFileText
		print "Current Sub Line: "+parameter[0]+" Len:"+str(len(reconstFileText)-1)
                for i in range (int(parameter[0])-decrementFlag,len(reconstFileText)-1):
	     	   reconstFileText[i] = reconstFileText[i+1]
                   print str(i+1)+"into"+str(i)
                del reconstFileText[i+1]   
                print "Removed Line: "+str(int(parameter[0])-decrementFlag)+" Deleted Node: "+str(i+1)
                print reconstFileText
                decrementFlag=decrementFlag+1
          currentJrnlLine=currentJrnlLine+1
          line= j.readline()
       j.close()
       print reconstFileText
       
       w=open(dr2+'/'+firstSplit[1],'w')
       for key in reconstFileText:
          w.write(str(key)+' '+reconstFileText[key]+'\n')
       w.close()
    #n = 1; target = newname(n)
    #while os.path.exists(target):
    #    n = n+1; target = newname(n)
    #shutil.move(source, target)
#time.sleep(t)
