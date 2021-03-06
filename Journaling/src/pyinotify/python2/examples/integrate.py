#!/usr/bin/env python
import shutil
import os
import time



def update_global_record(global_rec_file,inode_no,line_no,type_of_update):
	print "inode: "+str(inode_no) + "line: " + str(line_no) + "type: " + type_of_update
        if type_of_update == 'last_line_written_to_file':
           tempgRFile = []
           with open(global_rec_file) as fil:
              tempgRFile = fil.read().splitlines()
           #update list
           for k in range(0,len(tempgRFile)):
              globalSplit = tempgRFile[k].split(',')
	      #print globalSplit
              if globalSplit[1].strip('\n') == str(inode_no):
                 globalSplit[3]= str(line_no-1)
                 tempgRFile[k] = globalSplit[0]+','+globalSplit[1]+','+globalSplit[2]+','+globalSplit[3]+','+globalSplit[4]
           #write back to globalRecord file
           updategRec = open(global_rec_file,"w")
           for l in range(0,len(tempgRFile)):
              updategRec.write(tempgRFile[l] + '\n')
           updategRec.close() 


def get_global_record(global_rec_file,inode_no,stat_req):
	#print "inode: "+str(inode_no) + "line: " + str(line_no) + "type: " + type_of_update
        if stat_req == 'get_last_line_written_to_file':
           tempgRFile = []
           tempFilename = []
           with open(global_rec_file) as fil:
              tempgRFile = fil.read().splitlines()
           #update list
           for k in range(0,len(tempgRFile)):
              globalSplit = tempgRFile[k].split(',')
	      #print globalSplitflr
              if globalSplit[1].strip('\n') == str(inode_no):
                 #global_rec_file.close()
                 return globalSplit[3]

        elif stat_req == 'get_last_line_written_to_jrnl':
           tempgRFile = []
           tempFilename = []
           with open(global_rec_file) as fil:
              tempgRFile = fil.read().splitlines()
           #update list
           for k in range(0,len(tempgRFile)):
              globalSplit = tempgRFile[k].split(',')
	      #print globalSplitflr
              if globalSplit[1].strip('\n') == str(inode_no):
                 #global_rec_file.close()
                 return globalSplit[2]



        elif stat_req == 'get_filename':
           tempgRFile = []
           with open(global_rec_file) as fil:
              tempgRFile = fil.read().splitlines()
           #update list
           for k in range(0,len(tempgRFile)):
              globalSplit = tempgRFile[k].split(',')
	      #print globalSplitflr
              if globalSplit[1].strip('\n') == str(inode_no):
                 tempFilename = globalSplit[0].split('/')
                 #global_rec_file.close()
		 print tempFilename
                 return tempFilename[5]





#--- set the time interval to check for new files (in seconds) below 
#    this interval should be smaller than the interval new files appear!
t = 1
#--- set the source directory (dr1) and target directory (dr2)
dr1 = "/home/seecs/Documents/Journal"
dr2 = "/home/seecs/Documents/AJFS"

#name = "output_"; extension = ".txt"
#newname = lambda n: dr2+"/"+name+str(n)+extension

#while True:
inode_id = []
reconstFileText = {}
line = ""
newfiles = os.listdir(dr1)
for file in newfiles:
    print "Found Journal: "+file
    if file == "A-915800":
       inode_id = file.split('-')

       last_written_counter = get_global_record('/home/seecs/Documents/global/globalRecord',inode_id[1],'get_last_line_written_to_file')
       last_written_jrnl = get_global_record('/home/seecs/Documents/global/globalRecord',inode_id[1],'get_last_line_written_to_jrnl')
       journal_associated_file = get_global_record('/home/seecs/Documents/global/globalRecord',inode_id[1],'get_filename')	
      
       print "LAST LINE WRITTEN TO JRNL: ", last_written_jrnl
       print "LAST LINE WRITTEN TO FILE: ", last_written_counter
       print "ASSOCIATED FILENAME: ", journal_associated_file
       currentJrnlLine=0
       if last_written_counter >= last_written_jrnl:
          print "Nothing to integrate...exiting"
          continue
       #OPEN JOURNAL
       j=open(dr1+'/'+file,'r')
       
       #if file exists in AJFS, READ, SKIP AS MANY LINES
       if os.path.isfile(dr2+'/'+journal_associated_file):
          #LOAD File in memory
          print "EXISTING FILE FOUND"
          existing_file = open(dr2+'/'+journal_associated_file,'r')
          file_line = existing_file.readline()
	  tempCount = 0
          while file_line !="":
             reconstFileText[tempCount]=file_line.strip('\n')
             tempCount=tempCount+1
	     file_line = existing_file.readline()
             #Update currentJrnlLine to last_written_counter + 1
          currentJrnlLine = int(last_written_counter)
          for q in range(0,int(last_written_counter)+1):
             line = j.readline()
             print "Skipping Journal line:", q
          print "LOADED EXISTING FILE...REPLAY JOURNAL FROM LINE", last_written_counter
          print reconstFileText
       else:
          print "NO EXISTING FILE FOUND...REPLAY COMPLETE JOURNAL"
          line = j.readline()
       
       print "JOURNAL START LINE"
       print line
       filename = ""
       filename_counter = 0
       while line!="": #increment journal lines
	  if 1==1:#currentJrnlLine > last_written_counter: #start from after the line that was written last####
             print "Current Journal Line: "+str(currentJrnlLine)
             firstSplit = line.split(',') # split record
             #print firstSplit[1]
	     if filename_counter == 0:
	        filename = firstSplit[1]
	        print "FILE: " + filename
	        filename_counter = 1
             #print firstSplit
             secondSplit = firstSplit[3].split('|') # split changes
	     if '\n' in secondSplit:
                #secondSplit.remove('\n') # handle some formating issues
		secondSplit = [x for x in secondSplit if x != '\n']
		#print "Removed \\n"
             if '' in secondSplit:
                #secondSplit.remove('')
		secondSplit = [x for x in secondSplit if x != '']
		#print "Removed blank space"
             #print secondSplit
             decrementFlag=0 #to keep track of multiple removes in a single transaction and add after remove
             for change in secondSplit: #implement each change individually
                parameter = change.split(chr(168)) # change parameters
	        if parameter[1] == '+' and currentJrnlLine == 0:
	           reconstFileText[int(parameter[0])]=parameter[2] #write first line as it is from jrnl
		   print "First line written as it is..."
	        elif parameter[1] == '+' and currentJrnlLine >= 1: #accomodate +'s
                   print reconstFileText
		   print "Current Change Line: "+parameter[0]+" Total Changes: "+str(len(secondSplit))+" decFlag: "+str(decrementFlag)
                   for i in range (len(reconstFileText),int(parameter[0])-decrementFlag,-1):
	     	      reconstFileText[i] = reconstFileText[i-1]
                      print str(i-1)+"into"+str(i)
                   reconstFileText[int(parameter[0])-decrementFlag]=parameter[2]    
                   print "Added Line: "+str(int(parameter[0])-decrementFlag)
                   #print reconstFileText
	        #elif parameter[1] == '-' and currentJrnlLine == 0 and decrementFlag == 0:
	        #   print "Ignoring remove line..."
 	        #elif parameter[1] == '-' and currentJrnlLine == 0 and decrementFlag > 0:
                
		elif parameter[1] == '-' and currentJrnlLine == 0:
		   print "Ignoring deleted line...first line of journal" 
		   reconstFileText[int(parameter[0])]=""               
   
                elif parameter[1] == '-' and currentJrnlLine >=1:
                   print reconstFileText
		   print "Current Change Line: "+parameter[0]+" Total Changes: "+str(len(secondSplit))+" decFlag: "+str(decrementFlag)
                   i=0
                   for i in range (int(parameter[0])-decrementFlag,len(reconstFileText)-1):
	     	      reconstFileText[i] = reconstFileText[i+1]
                      print str(i+1)+"into"+str(i)
                      print reconstFileText
                   #print "Current Change Line: ",parameter[0]
		   #if int(parameter[0]) == len(reconstFileText)-1:#if last line is to be deleted
                   print "Deleting:  ",len(reconstFileText)-1
		   del reconstFileText[len(reconstFileText)-1]#int(parameter[0])-decrementFlag]
                   print reconstFileText
		   #reconstFileText[int(parameter[0])-decrementFlag] = ""
		   print "Removed Line-: "+str(int(parameter[0])-decrementFlag)+" Deleted Node: "+str(int(parameter[0])-decrementFlag)
		   #else:
		   #  if int(parameter[0])-decrementFlag in reconstFileText: #delete if exists
                         #del reconstFileText[int(parameter[0])-decrementFlag]#i+1]
			 #reconstFileText[int(parameter[0])-decrementFlag] = ""
                   #     print reconstFileText
                         #print "ELSE: Removed Line: "+str(int(parameter[0])-decrementFlag)+" Deleted Node: "+str(i+1)
                   print reconstFileText
                   decrementFlag=decrementFlag+1
          currentJrnlLine=currentJrnlLine+1
	  decrementFlag=0
          line= j.readline()
       j.close()
       print reconstFileText


       update_global_record('/home/seecs/Documents/global/globalRecord',inode_id[1],currentJrnlLine,'last_line_written_to_file')
       

       #write/update file in AJFS folder
       w=open(dr2+'/'+filename,'w')
       #for key in reconstFileText:
       #   w.write(str(key)+' '+reconstFileText[key]+'\n')
       #WRITE FILE IN ORDER
       #print reconstFileText
       for key in range(min(reconstFileText.keys(), key=int),max(reconstFileText.keys(), key=int)+1):
          #print "Key: ", key
	  if key in reconstFileText:
             #w.write(str(key)+' '+reconstFileText[key]+'\n')
             w.write(reconstFileText[key]+'\n')  

       w.close()
    #n = 1; target = newname(n)
    #while os.path.exists(target):
    #    n = n+1; target = newname(n)
    #shutil.move(source, target)
#time.sleep(t)

