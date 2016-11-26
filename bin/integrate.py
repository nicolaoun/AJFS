#!/usr/bin/env python
import shutil
import os
import time



def update_global_record(global_rec_file,inode_no,line_no,type_of_update):
	print("integrate func:update_global_record")
	#print "inode: "+str(inode_no) + "line: " + str(line_no) + "type: " + type_of_update
        if type_of_update == 1: #last_line_written_to_file=1
           tempgRFile = []
           with open(global_rec_file) as fil:
              tempgRFile = fil.read().splitlines()
           #update list
           for k in range(0,len(tempgRFile)):
              globalSplit = tempgRFile[k].split(',')
	      #print globalSplit
              if globalSplit[1].strip('\n') == str(inode_no):
	         if (int(globalSplit[3]) > -1):
                    globalSplit[3]= str(line_no-1)
		 else:
		    globalSplit[3] = str(-1)
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


        elif stat_req == 'get_jrnl_ver':
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
                 return tempFilename[7]


def merge_journals(CURR_CLIENT__,inodeid__):
        
	dr1 = "client_"+CURR_CLIENT__+"/Journal"
	dr2 = "client_"+CURR_CLIENT__+"/wf"
        dr3 = "client_"+CURR_CLIENT__+"/receive"
        print "Merge..."
	#get_last_line_written_to_file'
        last_written = get_global_record('client_'+CURR_CLIENT__+'/global/globalRecord',str(inodeid__),'get_last_line_written_to_file')
	last_written_to_jrnl = get_global_record('client_'+CURR_CLIENT__+'/global/globalRecord',str(inodeid__),'get_last_line_written_to_jrnl')
   	#open received and local journal
        rcvd_jrnl=open(dr3+'/A-'+str(inodeid__),'r')
        local_jrnl=open(dr1+'/A-'+str(inodeid__),'w')
        #skip lines from received journal
        for q in range(0,int(last_written)+1):
	   line = rcvd_jrnl.readline()
	   print "Skipping Received Journal line:", q
        
        #start appending lines from received journal to local journal
        jrnl_line = rcvd_jrnl.readline()
        while jrnl_line !="":
	   local_jrnl.write(jrnl_line)
           jrnl_line = rcvd_jrnl.readline()
        rcvd_jrnl.close()
	local_jrnl.close() 
	
        #call integrate journal to \wf
        integrate_jrnl(CURR_CLIENT__, inodeid__,"local")
        #write jrnl to shared memory in calling function
        

def integrate_jrnl(CURR_CLIENT_,inodeid_,jrnl_src): #NOT INTEGRATING
 	print("func:integrate_jrnl()")
	print(inodeid_)
        #STILL NEED TO TAKE NEW LINES AND APPEND AT THE END OF LOCAL JOURNAL
	#--- set the time interval to check for new files (in seconds) below 
	#    this interval should be smaller than the interval new files appear!
	t = 1
	#--- set the source directory (dr1) and target directory (dr2)
        #"client_"+CURR_CLIENT+"/Journal/A-"+str(inodeid_)
	dr1 = "client_"+CURR_CLIENT_+"/Journal"
	dr2 = "client_"+CURR_CLIENT_+"/wf"
        dr3 = "client_"+CURR_CLIENT_+"/receive"

	#name = "output_"; extension = ".txt"
	#newname = lambda n: dr2+"/"+name+str(n)+extension

	#while True:
	inode_id = []
	reconstFileText = {}
	line = ""
	newfiles = os.listdir(dr1)
	for i in range(0,1):#file in newfiles:
	    #print "Integrating Journal: A-"+str(inodeid_)
	    #if file == "A-915800":
            if 1==1:
	       #inode_id = file.split('-')

	       last_written_counter = get_global_record('client_'+CURR_CLIENT_+'/global/globalRecord',str(inodeid_),'get_last_line_written_to_file')
	       last_written_jrnl = get_global_record('client_'+CURR_CLIENT_+'/global/globalRecord',str(inodeid_),'get_last_line_written_to_jrnl')
	       journal_associated_file = get_global_record('client_'+CURR_CLIENT_+'/global/globalRecord',str(inodeid_),'get_filename')	#FIX
	       print "Journal Associated File: "+ dr2+'/'+journal_associated_file
	       print "LAST LINE WRITTEN TO JRNL: ", last_written_jrnl
	       print "LAST LINE WRITTEN TO FILE: ", last_written_counter
	       print "ASSOCIATED FILENAME: ", journal_associated_file
	       currentJrnlLine=0
	       if last_written_counter >= last_written_jrnl:
		  print "Nothing to integrate...exiting"
		  continue
	       #OPEN JOURNAL
               if jrnl_src=="rcv":
    	          j=open(dr3+'/A-'+str(inodeid_),'r')
               elif jrnl_src=="local":
                  j=open(dr1+'/A-'+str(inodeid_),'r')
	       
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
		           #print reconstFileText
		           decrementFlag=decrementFlag+1
		  currentJrnlLine=currentJrnlLine+1
		  decrementFlag=0
		  line= j.readline()
	       j.close()
	       print reconstFileText


	       update_global_record('client_'+CURR_CLIENT_+'/global/globalRecord',inodeid_,currentJrnlLine,1)
	       

	       #write/update file in wf folder
	       #w=open(dr2+'/'+filename,'w')
               w=open(dr2+'/'+journal_associated_file, 'w')

	       for key in range(min(reconstFileText.keys(), key=int),max(reconstFileText.keys(), key=int)+1):
		  #print "Key: ", key
		  if key in reconstFileText:
		     #w.write(str(key)+' '+reconstFileText[key]+'\n')
		     w.write(reconstFileText[key]+'\n')  

	       w.close()


