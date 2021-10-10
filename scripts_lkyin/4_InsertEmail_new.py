from __future__ import with_statement
import time
import unicodedata
import codecs
import os, sys, re, time, urllib2, urllib
import xml.dom.minidom
import threading
import Queue
import email
import pg8000
import logging
import optparse
import gc
from operator import itemgetter
from interp import _
from time import sleep



#Fpath=os.path.abspath(os.path.join(os.path.dirname('Main_Parse_InsertData.py'),os.path.pardir))
db_config=open('./config/db_config')
dbHOST=db_config.readline().split('\"')[1]
dbUSER=db_config.readline().split('\"')[1]
dbPASS=db_config.readline().split('\"')[1]
dbDB=db_config.readline().split('\"')[1]
db_config.close()
conn = pg8000.connect(host=dbHOST,user=dbUSER,password=dbPASS,database=dbDB)
#conn = pg8000.connect(host="localhost",user="postgres",password="password",database="postgres")
mailtype='dev-' #emails among developers
FILELIST='mbox_filelist.txt'
TABLE_POSTFIX='_2019_3'
Fpath='/data/Apachembox/'

def getMonthContent(path):
     #try:
          fileContentIn=codecs.open(path)
          global ContentIn
          ContentIn=fileContentIn.readlines()
          fileContentIn.close()

          messages=[]
          messageText=""
          for i in range(len(ContentIn)):
               if (i+1 < len(ContentIn) and ContentIn[i].startswith("From ") and ContentIn[i+1].startswith("Return-Path:")) or (i+1==len(ContentIn)):
                    if len(messageText):
                         message = email.message_from_string(messageText)
                         #print "Message", message["from"], message["message-id"]
                         messages.append(message)
                    messageText = ""
               else:
                    messageText += ContentIn[i]
          print "there are", len(messages), " messages"," ",path
          #global totalMessages
          #totalMessages = totalMessages + len(messages)
          if len(messages) == 0:
               return [""]
          return messages

     #except:
      #    print "ERROR\r\n"

def insertContent(path,PJstatus,messages,conn,need_body):
     #try:
         
          
          project='ApacheMailArchives'
          listname=PJstatus[1]
          cursor = conn.cursor()
          

          listidIn=PJstatus[0]
          
          # get all of the aliases and messageids currently in the database
          insertedMessageIds = set()
          #sql = "select messageid from messages"+TABLE_POSTFIX+" where list = '"+str(listidIn)+"'"
          try:
              cursor.execute(_("select messageid from messages"+TABLE_POSTFIX+" where list = '"+str(listidIn)+"'"))
              result=cursor.fetchall()
          except pg8000.ProgrammingError,e:
              print e
          for row in result:
               insertedMessageIds.add(row[0])
          
          aliases = {}
          cursor.execute("select name, email, aliasid from aliases"+TABLE_POSTFIX)
          result=cursor.fetchall()
          for name, address, aliasId in result:
               aliases[name, address] = aliasId
          

          #first insert all of the aliases
          unknownAliases = set()
          for message in messages:
               if "from" in message:
                    name, address = email.utils.parseaddr(message["from"])
                    name = asciiCodify(name)
                    if not aliases.has_key( (name, address) ):
                         unknownAliases.add( (name, address) )

          if len(unknownAliases) > 0:
               sql = "insert into aliases"+TABLE_POSTFIX+" (aliasid, name, email) values " + \
                    ", ".join(["(DEFAULT, '" + escape(name) + "', '" + escape(address) + "')" for name, address in unknownAliases]) + \
                    " returning name, email, aliasid"
#               print sql
               cursor.execute(sql)
               result=cursor.fetchall()
               for name, address, id in result:
                    aliases[name, address] = id
               conn.commit()

#          print aliases

          errorsLog = open("errors", "w")
        
          # Now insert all of the messages
          for message in messages:
               if not "message-id" in message:
                    continue
               messageId = fixMsgId(message["message-id"])
#               print messageId
               if not "from" in message:
                    print messageId, "no from in message"
                    print >> errorsLog, message, "no from in message"
                    continue

               # if we've already put it in the database then skip it
               if messageId in insertedMessageIds:
                    continue
               insertedMessageIds.add(messageId)
               name, address = email.utils.parseaddr(message["from"])
               name = asciiCodify(name)
               aliasId = aliases[name, address]
               
               if message.is_multipart():
                    body = "EMPTY"
                    for subMessage in message.get_payload():
                         if subMessage.get_content_type() == "text/plain":
                              body = escape(subMessage.get_payload())
                              break;
               else:
                    body = escape(message.get_payload())

               subject = escape(message["subject"])
               try:
                    if len(message["date"].split(" "))>4:
                         datetime = message["date"]
                    if '(' in datetime:
                         delLen=-len(datetime.split("(")[1])-2
                         datetime=datetime[:delLen]
                    tempdate=str(re.search(r'([0-9]+) ([\w]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([\W]+[0-9]+)', str(datetime)).group())
                    tempdate=tempdate.split(' ')
                    if len(tempdate[0])<4 and len(tempdate[2])<4:
                         tempdate[2]="20"+tempdate[2]
                         datetime=' '.join(tempdate)
               except:
                    #print message
                    # Date not in usual placement, try this placement
                    # Extracting date of type "From nobody Sat Mar 17 14:02:11 2012"
                    datetime = str(re.search(r'([0-9]+) ([\w]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([\W]+[0-9]+)', str(message)).group())
                    
                    
               
               inReplyTo = "NULL"
               if "in-reply-to" in message:
                    inReplyTo = escape(fixMsgId(message["in-reply-to"]))
               if need_body==True:
                   sql = "insert into messages"+TABLE_POSTFIX+" (list, messageid, subject, body, senderaliasid, datetime, referenceid,senderalias) values "+\
                    "('"+str(listidIn)+"', '"+str(messageId)+"', '"+str(subject)+"', '"+str(body)+"', '"+str(aliasId)+"','"+datetime+"', '"+inReplyTo+"','"+str(name)+"')"
               else:
                   sql = "insert into messages"+TABLE_POSTFIX+" (list, messageid, subject, senderaliasid, datetime, referenceid,senderalias) values "+\
                    "('"+str(listidIn)+"', '"+str(messageId)+"', '"+str(subject)+"', '"+str(aliasId)+"','"+datetime+"', '"+inReplyTo+"','"+str(name)+"')"
               
#               print "Inserting", messageId
               
               try:
                    cursor.execute(sql)
                    
               except pg8000.ProgrammingError,e:
                    #print message
                    print "insert error"
                    print >> errorsLog,e
                    print >> errorsLog, sql
               conn.commit()

          

               
     # Later look into using this to debug:
     # In CPython, at least on Windows, you can run the interpreter using the -i command-line parameter, 
     # e.g: python -i myscript.py It gives you an interactive console after the program is finished. This 
     # is handy for inspecting the state of the program, and it also works in case of an unhandled exception.
     #except Exception as inst:
      #    logging.debug(str(type(inst)) + " ERROR: while parsing " + project + ". \n")
       #   logging.debug("The list was " + str(list) + "\n")
          #logging.debug("The messages grabbed are " + str(messages) + "\n")



# Converts text into ascii to fix weird characters
def asciiCodify(s):
    try:
        u = unicode(s, errors="replace")
        v = u.encode("ascii", "replace")
        return v
    except:
        # s is empty
        return "EMPTY"
# Fix any characters that might mess up script
def escape(s):
    return asciiCodify(s).replace("'", "''").replace('%', '%%')
    
# Counts number of lines in file
def file_line_number(file):
    for i, l in enumerate(file):
        pass
    return i + 1

def purifyNum2str(stringIn):
     In=str(stringIn)
     Num=[]
     for i in range(len(In)):
          try:
               Num.append(int(In[i]))
          except:
               continue
     output=""
     for i in range(len(Num)):
          output+=str(Num[i])
     return output

#Getting rid of certain characters in message ids.
def fixMsgId(id):
    return id.replace("<", "").replace(">", "").replace("%", "_")


######################################################################

# MAIN START HERE

######################################################################

#read projects' status
ListAliase = conn.cursor()
ListAliase.execute(_("select * from lists"+TABLE_POSTFIX))
PJstatusIn=ListAliase.fetchall()
PJstatus=[]
PJnames=[]
for i in range(len(PJstatusIn)):
     if PJstatusIn[i][5]!=False:
          PJstatus.append(PJstatusIn[i])
          PJnames.append(PJstatusIn[i][2])
ListAliase.close()

#read file list
fileListIn=codecs.open(FILELIST)
pathListIn=fileListIn.read().splitlines()# the path list for files
fileCount=len(pathListIn)
fileListIn.close()

#PJnameList=[]#the list of project names for mbox files

not_in_pj=[]
pathList=[]
for i in range(0,fileCount):
    pathIn=pathListIn[i]
    pjname=pathIn.split('/')[-1].split('-')[0]
    if mailtype in pathIn:#check if the file is for dev (emails)
        try:
            pj_num_id=PJnames.index(pjname)
            #PJnameList.append([PJstatus[pj_num_id][0],PJstatus[pj_num_id][2]])
            pathList.append([Fpath+pathIn[2:],PJstatus[pj_num_id][0],PJstatus[pj_num_id][2]])#path,listid,name
            
        except:
            not_in_pj.append(pjname)
            continue
     #pathList[i]=ListIn[i].split('\r\n')[0]
not_in_pj=set(not_in_pj)
print len(not_in_pj)
print pathList[0]
pathList=sorted(pathList,key=itemgetter(0))#in a time flow order, though files with 'incubator' may list before the other in a project
for i in range(0,len(pathList)):
     pjname=pathIn.split('/')[-1].split('-')[0]


#x12=1/0##################### BREAK POINT
print "Read all necessary files successfully!\r\n"

count_files=len(pathList)
need_body=True
for x in range(0,count_files):

     #Content=getMonthContent(pathList[x][0])


     Content=[]
     fileContentIn=codecs.open(pathList[x][0])

     ContentIn=fileContentIn.readlines()
     fileContentIn.close()

     Content=[]
     messageText=""
     for i in range(len(ContentIn)):
          if (i+1 < len(ContentIn) and ContentIn[i].startswith("From ") and ContentIn[i+1].startswith("Return-Path:")) or (i+1==len(ContentIn)):
               if len(messageText):
                    message = email.message_from_string(messageText)
                    #print "Message", message["from"], message["message-id"]
                    Content.append(message)
               messageText = ""
          else:
               messageText += ContentIn[i]
     print "there are", len(Content), " messages"
          #global totalMessages
          #totalMessages = totalMessages + len(messages)
     if len(Content) == 0:
          Content=[""]


     
     #for i in range(3):# only try 3 times
     while 1:
          try:
               insertContent(pathList[x][0],pathList[x][1:],Content,conn,need_body)
               break
          except:
                                                                
               print 'Time Out. Try Again.'
               conn.commit()
               sleep(1)
               continue
     print str(x+1)+"    "+str(count_files)+"\n"
     del ContentIn
     gc.collect()
#Content=getMonthContent(Fpath,pathList[15])
conn.close()

