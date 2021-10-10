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

from interp import _
from time import sleep
TABLE_POSTFIX='_2019_8'
#TABLE_POSTFIX=''
need_body=True
db_config=open('./config/db_config')
dbHOST=db_config.readline().split('\"')[1]
dbUSER=db_config.readline().split('\"')[1]
dbPASS=db_config.readline().split('\"')[1]
dbDB=db_config.readline().split('\"')[1]
db_config.close()
conn = pg8000.connect(host=dbHOST,user=dbUSER,password=dbPASS,database=dbDB)
conn.close()

Fpath='/data/Apachembox/'
#Fpath='./'
FILELIST='mbox_filelist.txt'
mailtype=['commits-','dev-'] #commits

def getMonthContent(Fpath,path):
     #try:
          fileContentIn=codecs.open(Fpath+path)
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
          print "there are", len(messages), " messages"
          #global totalMessages
          #totalMessages = totalMessages + len(messages)
          if len(messages) == 0:
               return [""]
          return messages

     #except:
      #    print "ERROR\r\n"

def getKeysByValue(dictOfElements, valueToFind):
    listOfKeys = list()
    listOfItems = dictOfElements.items()
    for item  in listOfItems:
        if item[1] == valueToFind:
            listOfKeys.append(item[0])
    return  listOfKeys



# Converts text into ascii to fix weird characters
def asciiCodify(s):
    try:
        u = unicode(s, errors="replace")
        v = u.encode("ascii", "replace")
        return v
    except:
        # s is empty
        return "EMPTY"
# Fix any characters that might mess up script, replace \x with ?
def escape(s):
    return re.sub(r'[^\x00-\x7f]',r'?',asciiCodify(s).replace("'", "''").replace('%', '%%').replace('\r',''))
    

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
def insertsql(sql,cursor):
     try:
          cursor.execute(sql)
          conn.commit()
     except pg8000.ProgrammingError, e:
          ##print message
          #print sql
          #print e
          #print "insert commit error"
          
          if u'23505' not in str(e):
              print >> errorsLog, e
              print >> errorsLog, sql
          
     conn.commit()

def check_str_list(strin,listin):#if listin contains a substring of strin, return the index
    for i in range(len(listin)):
        if listin[i] in strin:
            return i
    return -1

def Get_commit_brief(message):#cname,caddress,aname,aaddress,msg_format

     #is svn, author = committer
     if message['X-Mailer'] is not None and message['X-Mailer'][0:3]!='ASF':
          try:
               name, address = email.utils.parseaddr(message["from"])
               if name=='':
                    try:
                         namepos1=str(message).index("\nAuthor:")
                         name=str(message)[namepos1:namepos1+100].split("\n")[1].split(" ")[1]
                         name = asciiCodify(name)
                    except:
                         name=address.split('@')[0]
               return escape(name),escape(address),escape(name),escape(address),'svn'
          except:
               return None, None,None,None,None

     #is git
     elif message['X-Mailer'] is not None or 'X-Git-' in message :
          try:
               
               namepos1=str(message).index("\nAuthor:")
               ccount=9
               while(str(message)[namepos1+ccount]!='\n'):
                    ccount=ccount+1                
               aname=str(message)[namepos1+9:namepos1+ccount].split(' <')[0]
               aaddress=str(message)[namepos1+9:namepos1+ccount].split(' <')[1].split('>')[0]
          except:
               cname, caddress = email.utils.parseaddr(message["from"])
               cname = caddress.split('@')[0]
               return escape(cname),escape(caddress),None,None,'git'
          try:
               namepos2=str(message).index("\nCommitter:")
               ccount=12
               while(str(message)[namepos2+ccount]!='\n'):
                    ccount=ccount+1                
               cname=str(message)[namepos2+12:namepos2+ccount].split(' <')[0]
               caddress=str(message)[namepos2+12:namepos2+ccount].split(' <')[1].split('>')[0]
          except:
               cname, caddress = email.utils.parseaddr(message["from"])
               cname = caddress.split('@')[0]
          return escape(cname),escape(caddress),escape(aname),escape(aaddress),'git'
          
               

          

     #is email
     elif "from" in message:
          name, address = email.utils.parseaddr(message["from"])
          name = asciiCodify(name)
          if name=='':
               name=address.split('@')[0]
          return escape(name),escape(address),None,None,'ema'
     else:
          return None,None,None,None,None
     
######################################################################

# MAIN START HERE

######################################################################
conn = pg8000.connect(host=dbHOST,user=dbUSER,password=dbPASS,database=dbDB)

#read projects' status
ListAliase = conn.cursor()
ListAliase.execute(_("select * from lists"+TABLE_POSTFIX))
PJstatusIn=ListAliase.fetchall()
PJstatus=[]
PJnames=[]
for i in range(len(PJstatusIn)):
     if PJstatusIn[i][6]!=False:
          PJstatus.append(PJstatusIn[i])
          PJnames.append(PJstatusIn[i][2])
ListAliase.close()

#read file list
fileListIn=codecs.open(FILELIST)
pathListIn=fileListIn.read().splitlines()# the path list for files
fileCount=len(pathListIn)
fileListIn.close()

PJnameList=[]#the list of project names for mbox files

not_in_pj=[]
pathList=[]
for i in range(0,fileCount):
    pathIn=pathListIn[i]
    pjname=pathIn.split('/')[-1].split('-')[0]
    if any(ext in pathIn for ext in mailtype):#check if the file is for dev or commit (emails)
        try:
            PJnames.index(pjname)
            pathList.append(pathIn)
        except:
            not_in_pj.append(pjname)
            continue
     #pathList[i]=ListIn[i].split('\r\n')[0]
     
not_in_pj=set(not_in_pj)
pathList=sorted(pathList,key=str.lower)#in a time flow order
for i in range(0,len(pathList)):
    pj_num_id=check_str_list(pathList[i],PJnames)
    PJnameList.append([PJstatus[pj_num_id][0],PJstatus[pj_num_id][2]])


print "Read all necessary files successfully!\r\n"

file1=codecs.open('PJstatus2.txt','w+')
for zzz in range(len(PJnameList)):
     file1.write(str(PJnameList[zzz])+'\t'+str(pathList[zzz])+'\n')
file1.close()
#previousPJid=168
conn.close()
#X12=1/0##################### BREAK POINT


#auto reset alias and alias_commit primary key to 1 when the tables are blank
conn = pg8000.connect(host=dbHOST,user=dbUSER,password=dbPASS,database=dbDB)
reseter = conn.cursor()
sql='select * from aliases'+TABLE_POSTFIX+' limit 1'
reseter.execute(sql)
indicatorin=reseter.fetchall()
if len(indicatorin)==0: #reset aliases
    sql='ALTER SEQUENCE aliases'+TABLE_POSTFIX+'_aliasid_seq RESTART WITH 1'
    reseter.execute(sql)
    conn.commit()
    
sql='select * from messages'+TABLE_POSTFIX+' limit 1'
reseter.execute(sql)
indicatorin=reseter.fetchall()
if len(indicatorin)==0: #reset aliases
    sql='ALTER SEQUENCE messages'+TABLE_POSTFIX+'_numid_seq RESTART WITH 1'
    reseter.execute(sql)
    conn.commit()

'''
sql='select * from aliases_commits'+TABLE_POSTFIX+' limit 1'
reseter.execute(sql)
indicatorin=reseter.fetchall()
if len(indicatorin)==0: #reset aliases_commit
    sql='ALTER SEQUENCE aliases_commits'+TABLE_POSTFIX+'_aliasid_seq RESTART WITH 1'
    reseter.execute(sql)
    conn.commit()
'''

conn.close()
    

#commitid=66774
#fileid=58591
previousPJid=0 # listid start from 1
commitid=0 #a numeric id for index
fileid=0 #unique within a project

insertedMessageIds = set()
aliases = {}
#social_aliases={}

errorsLog = open("errors", "w+")
errorsLog.close()

#read each mbox file (which contains a month of messages) and parse
for x in range(0,len(pathList)):
     conn = pg8000.connect(host=dbHOST,user=dbUSER,password=dbPASS,database=dbDB)

     #484 test x-git
     #115 test xmailer asf git
     #pathList[x]='./AAinsertData/test_commit_1.txt'
     #pathList[x]='./abdera.apache.org/commits-200802.mbox'#for test use
     #def insertContent(path,PJstatus,messages,conn):

     
     #Content=getMonthContent(Fpath,pathList[x])
     
     #Read all messages from a mbox file
     Content=[]
     fileContentIn=codecs.open(Fpath+pathList[x][2:])
     
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
     print "there are", len(Content), " messages    "+pathList[x]
          #global totalMessages
          #totalMessages = totalMessages + len(messages)
     if len(Content) == 0:
          Content=[""]



     
     #insertContent(pathList[x],PJnameList[x],Content,conn)######
     if True:#this was a test code, not used in this version
          path=pathList[x]
          PJstatus=PJnameList[x]
          messages=Content
          project='ApacheMailArchives'
          listname=PJstatus[1]
          cursor = conn.cursor()
          
          
          
          listidIn=PJstatus[0]


          #if begins to parse a mbox from another project
          #read ids from database in case it is an update of data
          if previousPJid!=listidIn:#this is to reset commitid and file id for different projects
               #commitid=0
               #fileid=0
               sql="select max(fileid) from filelist"+TABLE_POSTFIX+" where list="+str(listidIn)
               cursor.execute(sql)
               fileid=cursor.fetchall()[0][0]
               if fileid==None:
                    fileid=0
               
               sql="select max(id) from commits"+TABLE_POSTFIX+" where list="+str(listidIn)
               cursor.execute(sql)
               commitid=cursor.fetchall()[0][0]
               if commitid==None:
                    commitid=0
                         
               # get all of the aliases and messageids currently in the database
               insertedMessageIds = set()
                     
               cursor.execute(_("select messageid from commits"+TABLE_POSTFIX+" where list = '"+str(listidIn)+"'"))
               result=cursor.fetchall()
               for row in result:
                    insertedMessageIds.add(row[0])

               cursor.execute(_("select messageid from messages"+TABLE_POSTFIX+" where list = '"+str(listidIn)+"'"))
               result=cursor.fetchall()
               for row in result:
                    insertedMessageIds.add(row[0])
          
               aliases = {}
               '''
               cursor.execute("select name, email, aliasid from aliases_commits"+TABLE_POSTFIX)
               result=cursor.fetchall()
               for name, address, aliasId in result:
                    aliases[name,address] = aliasId
                             
               social_aliases={}
               '''
               cursor.execute("select name, email, aliasid from aliases"+TABLE_POSTFIX)
               result=cursor.fetchall()
               for name, address, aliasId in result:
                    aliases[name,address] = aliasId
                    
                    
          previousPJid=listidIn

          from_commit='commits-' in path #bool, check if it is from developers' email archive
          

               
          
          
          #first insert all of the aliases
          #unknowEmail = {}#assume a email address map to an alias
          #unknownAliases = set()

#read all alias first (need their num id (or generate in the program))
          '''for message in messages:
               #first check if it is an email(social) or a commit
               
               cname,caddress,have_author_info=Get_name_addr(message)
               if cname==None:
                    continue
               if (not aliases.has_key( caddress ) ) and (not unknowEmail.has_key(caddress)) and (cname!='buildbot'):
                    unknowEmail[caddress]=1
                    unknownAliases.add((cname,caddress))
               if have_author_info==1:
                    aname,aaddress=Get_name_addr(message)[0:2]
                    if aname!=cname:
                         if (not aliases.has_key( aaddress ) ) and (not unknowEmail.has_key(aaddress)) and (aname!='buildbot'):
                              unknowEmail[aaddress]=1
                              unknownAliases.add((aname,aaddress))
                         
                    
          if len(unknownAliases) > 0:
               #print name,address
               #print escape(name),escape(address)
               #print unknownAliases
               sql = "insert into aliases_commits"+TABLE_POSTFIX+" (aliasid, name, email) values " + \
                    ", ".join(["(DEFAULT, '" + escape(name) + "', '" + escape(address) + "')" for name, address in unknownAliases]) + \
                    " returning name, email, aliasid"
               #print sql
               try:
                   cursor.execute(sql)
                   result=cursor.fetchall()
               except:
                   continue
               for name, address, id in result:
                    aliases[address] = id
               conn.commit()

#          print aliases'''

          errorsLog = open("errors", "a")

          # Now insert all of the messages
          for message in messages:
               
               
               if not "message-id" in message:
                    continue
               messageId = fixMsgId(message["message-id"])
#               print messageId
               if not "from" in message:
                    print messageId, "no FROM in message"
                    print >> errorsLog, message, "no FROM in message"
                    continue
               
               # if we've already put it in the database then skip it
               if messageId in insertedMessageIds:
                    continue
               insertedMessageIds.add(messageId)


               subject = escape(message["subject"]) #title of the message
               cname,caddress,aname,aaddress,msg_format=Get_commit_brief(message)
               if subject[0:3]=='Re:': #patches: some rare cases
                    msg_format='ema'
               elif subject[0:4]=='svn ':
                    msg_format='svn'
               
               #is an email
               if msg_format=='ema':#handle it as an email
                    if not aliases.has_key((cname,caddress)):
                         sql = "insert into aliases"+TABLE_POSTFIX+" (aliasid, name, email,from_commit) values " + \
                         "(DEFAULT, '" + escape(cname) + "', '" + escape(caddress) + "',False)"  + \
                         " returning name, email, aliasid"
                         try:
                              cursor.execute(sql)
                         except pg8000.ProgrammingError,e:
                         #print message
                              if u'23505' not in str(e):
                                   print >> errorsLog, e
                                   print >> errorsLog, sql
                                   print "insert email error"
                              
                         result=cursor.fetchall()
                         for name, address, id in result:
                              aliases[name, address] = id
                         conn.commit()
                    try:     
                         aliasId=aliases[cname, caddress]
                    except e:
                         
                         print >> errorsLog, e
                         print >> errorsLog, cname
                         print >> errorsLog, caddress
                         

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
                    receiverid_in=0
                    receiveralias_in="NULL"
                    if "in-reply-to" in message:
                         inReplyTo = escape(fixMsgId(message["in-reply-to"]))
                         
                         #find receiver id and alias
                         
                         try:
                              sql="select senderaliasid, senderalias from messages"+TABLE_POSTFIX+" where messageid='"+inReplyTo+"' limit 1"
                              cursor.execute(sql)
                              receiverid_in,receiveralias_in=cursor.fetchall()[0]
                         except:
                              try:
                                   sql="select aliasid, name from aliases"+TABLE_POSTFIX+" where aliasid=(select committerid from commits"+TABLE_POSTFIX+\
                                       " where messageid='"+inReplyTo+"' limit 1) limit 1"
                                   cursor.execute(sql)
                                   receiverid_in,receiveralias_in=cursor.fetchall()[0]
                              except:
                                   receiverid_in=0
                                   receiveralias_in="NULL"
                    
                    if  receiverid_in!=0:
                         if need_body==True:
                             sql = "insert into messages"+TABLE_POSTFIX+" (list, messageid, subject, body, senderaliasid, datetime"+\
                                   ",referenceid,recipaliasid,recipalias,senderalias,from_commit) values "+\
                                   "('"+str(listidIn)+"', '"+str(messageId)+"', '"+escape(subject)+"', '"+escape(body)+"', '"+str(aliasId)+"','"+datetime+\
                                   "', '"+inReplyTo+"','"+str(receiverid_in)+"','"+str(receiveralias_in)+"','"+escape(cname)+"',"+str(from_commit)+")"
                         else:
                             sql = "insert into messages"+TABLE_POSTFIX+" (list, messageid, subject, senderaliasid, datetime"+\
                                   ",referenceid,recipaliasid,recipalias,senderalias,from_commit) values "+\
                                   "('"+str(listidIn)+"', '"+str(messageId)+"', '"+escape(subject)+"', '"+str(aliasId)+"','"+datetime+\
                                   "', '"+inReplyTo+"','"+str(receiverid_in)+"','"+str(receiveralias_in)+"','"+escape(cname)+"',"+str(from_commit)+")"
                    else:
                         if need_body==True:
                             sql = "insert into messages"+TABLE_POSTFIX+" (list, messageid, subject, body, senderaliasid, datetime"+\
                                   ",referenceid,senderalias,from_commit) values "+\
                                   "('"+str(listidIn)+"', '"+str(messageId)+"', '"+escape(subject)+"', '"+escape(body)+"', '"+str(aliasId)+"','"+datetime+\
                                   "', '"+inReplyTo+"','"+escape(cname)+"',"+str(from_commit)+")"
                         else:
                             sql = "insert into messages"+TABLE_POSTFIX+" (list, messageid, subject, senderaliasid, datetime"+\
                                   ",referenceid,senderalias,from_commit) values "+\
                                   "('"+str(listidIn)+"', '"+str(messageId)+"', '"+escape(subject)+"', '"+str(aliasId)+"','"+datetime+\
                                   "', '"+inReplyTo+"','"+escape(cname)+"',"+str(from_commit)+")"
     #               print "Inserting", messageId
                    
                    try:
                         cursor.execute(sql)
                         
                    except pg8000.ProgrammingError,e:
                         #print message
                         if u'23505' not in str(e):
                             print >> errorsLog, e
                             print >> errorsLog, sql
                             print "insert email error"
                         
                    conn.commit()

                    continue
               
               #finish email handling
               authorId=-1     
               if cname==None or cname=='buildbot':#skip bot, may have other kinds of bots
                    continue
               elif not aliases.has_key((cname,caddress)):
                    sql = "insert into aliases"+TABLE_POSTFIX+" (aliasid, name, email,from_commit) values " + \
                    "(DEFAULT, '" + escape(cname) + "', '" + escape(caddress) + "',True)" + \
                    " returning name, email, aliasid"
                    try:
                         cursor.execute(sql)
                         result=cursor.fetchall()
                    except pg8000.ProgrammingError,e:
                         print "insert committer alias error"
                         print >> errorsLog,e
                         print >> errorsLog, sql
                         
                         continue
                    for name, address, id in result:
                         aliases[name,address] = id
                    conn.commit()

               if aname!=None :#have author info in git
                    if not aliases.has_key((aname,aaddress)):
                         sql = "insert into aliases"+TABLE_POSTFIX+" (aliasid, name, email,from_commit) values " + \
                         "(DEFAULT, '" + escape(aname) + "', '" + escape(aaddress) + "',True)" + \
                         " returning name, email, aliasid"
                         try:
                              cursor.execute(sql)
                              result=cursor.fetchall()
                         except pg8000.ProgrammingError,e:
                              print "insert author alias error"
                              print >> errorsLog,e
                              print >> errorsLog, sql
                         
                              continue
                         for name, address, id in result:
                              aliases[name,address] = id
                         conn.commit()
                    authorId=aliases[aname,aaddress]
               
               #name = asciiCodify(name)
               #print aliases
               committerId = aliases[cname,caddress]

               if message.is_multipart():
                    body = "EMPTY"
                    for subMessage in message.get_payload():
                         if subMessage.get_content_type() == "text/plain":
                              body = escape(subMessage.get_payload())
                              break;
               else:
                    body = escape(message.get_payload())
               

               #get message datetime
               try:
                    if len(message["date"].split(" "))>4:
                         datetime = message["date"]
                    if '(' in datetime:
                         delLen=-len(datetime.split("(")[1])-2
                         datetime=datetime[:delLen]
               except:
                    #print message
                    # Date not in usual placement, try this placement
                    # Extracting date of type "From nobody Sat Mar 17 14:02:11 2012"
                    datetime = str(re.search(r'([0-9]+) ([\w]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([\W]+[0-9]+)', str(message)).group())
              
               #print str(body)
                    
               fileop=""
               filename=""
               addlines=0
    
               num=-1
               mlen=len(body)-1
               
               #if is Git (contain 'X-Mailer: ASF-Git Admin Mailer' or 'X-Git')
               if msg_format=='git':

               
                    try:
                         adate=escape(str(message).split("\nAuthored: ")[1].split("\n")[0])
                    except:
                         try:
                              adate=escape(str(message).split("\nAuthorDate: ")[1].split("\n")[0])
                         except:
                              adate=None
                    try:
                         cdate=escape(str(message).split("\nCommitted: ")[1].split("\n")[0])
                    except:
                         cdate=datetime

                    try:
                         try:
                              sha=escape(str(message).split("\nCommit: ")[2].split("\n")[0])
                         except:
                              sha=escape(str(message).split("\nX-Git-Rev: ")[1].split("\n")[0])
                    except:
                         sha=None
                         
                    
                    fileop=''
                    while 1:
                         num+=1
                         if num>=mlen-1:#finish phrasing message
                              break
                         elif (body[num-1]=='\n' and num>1) or num==0:
                              
                              if body[num:num+6]=='http:/':
                                   ccount=7
                                   while(body[num+ccount]!='\n'):
                                        ccount=ccount+1
                                   filename=escape(body[num+7:num+ccount])
                                   #print "filename here"
                                   #1/0
                              if body[num:num+6]=='diff -' and filename!='':
                                   flag_start=0
                                   while 1:# read file names before commit and after commit
                                        if num>=mlen:
                                             oldname=''
                                             break
                                        if flag_start==0 and body[num]=='\n':
                                             if body[num+1:num+5]=='--- ':
                                                  ccount=1
                                                  while(body[num+ccount]!='\n'):
                                                       ccount=ccount+1
                                                  oldname=body[num+6:num+ccount]
                                                  num=num+ccount
                                                  ccount=1
                                                  while(body[num+ccount]!='\n'):
                                                       ccount=ccount+1
                                                  newname=body[num+6:num+ccount]
                                                  num=num+ccount
                                                  break
                                        num+=1
                                   
                                   if oldname=='':
                                        break
                                   elif oldname==newname:#give a file operation based on filename changes
                                        fileop='mod'
                                        
                                        sql="select fileid from filelist"+TABLE_POSTFIX+" where filename='"+str(filename)+"' and isremoved is False limit 1"#check if file is in the list
                                        cursor.execute(sql)
                                        fileid_in=cursor.fetchone()
                                        if fileid_in is None:
                                        #if not, add it to the file list
                                             fileid+=1
                                             sql="insert into filelist"+TABLE_POSTFIX+" (list, filename,fileid) values"+\
                                                  "('"+str(listidIn)+"', '"+str(filename)+"', '"+\
                                                  str(fileid)+"')"
                                             insertsql(sql,cursor)
                                             fileid_in=fileid
                                        else:
                                             fileid_in=fileid_in[0]
                                        
                                   
                                   elif oldname=='dev/null\r' or oldname=='dev/null':
                                        fileop='add'
                                        fileid+=1
                                        sql="insert into filelist"+TABLE_POSTFIX+" (list, filename,fileid) values"+\
                                             "('"+str(listidIn)+"', '"+str(filename)+"', '"+\
                                             str(fileid)+"')"
                                        insertsql(sql,cursor)
                                        fileid_in=fileid
                                        
                                   elif newname=='dev/null\r' or newname=='dev/null':
                                        fileop='del'
                                        sql="update filelist"+TABLE_POSTFIX+" set isremoved=true where filename=\'"+str(filename)+"\'"
                                        insertsql(sql,cursor)
                                        fileid_in=0 #
                                   else:
                                        fileop='copy'
                                        sql="select fileid from filelist"+TABLE_POSTFIX+" where filename='"+str(filename)+"' and isremoved is False limit 1"#check if file is in the list
                                        cursor.execute(sql)
                                        fileid_in=cursor.fetchone()
                                        if fileid_in is None:
                                        #if not, add it to the file list
                                             fileid+=1
                                             sql="insert into filelist"+TABLE_POSTFIX+" (list, filename,fileid) values"+\
                                                  "('"+str(listidIn)+"', '"+str(filename)+"', '"+\
                                                  str(fileid)+"')"
                                             insertsql(sql,cursor)
                                             fileid_in=fileid
                                        else:
                                             fileid_in=fileid_in[0]
                                   addlines=0
                                   dellines=0
                                   while (num<mlen-6 and body[num:num+6]!='\nhttp:'):
                                        num=num+1
                                        if body[num:num+2]=='\n+' and body[num+2]!='+':
                                             addlines+=1
                                        elif body[num:num+2]=='\n-' and body[num+2]!='-':
                                             dellines+=1
                                   commitid+=1
                                   
                                   if authorId !=-1 and adate is not None and sha is not None:#if contains commit brief info
                                        sql = "insert into commits"+TABLE_POSTFIX+" (list, messageid, committerid, file_operation, file_name, datetime, addlines, dellines,id,file_id"+\
                                              ",authorid,author_datetime,commit_datetime,sha_or_rev,format) values "+\
                                             "('"+str(listidIn)+"', '"+str(messageId)+"', '"+str(committerId)+"', '"+str(fileop)+"','"+\
                                             str(filename)+"', '"+datetime+"','"+str(addlines)+"','"+str(dellines)+"', '"+str(commitid)+"', '"+str(fileid_in)+\
                                             "', '"+str(authorId)+"', '"+adate+"', '"+cdate+"', '"+str(sha)+"', 'git')"

                                   else:
                                        sql = "insert into commits"+TABLE_POSTFIX+" (list, messageid, committerid, file_operation, file_name, datetime, addlines, dellines,id,file_id"+\
                                             ",authorid,author_datetime,commit_datetime,sha_or_rev,format) values "+\
                                             "('"+str(listidIn)+"', '"+str(messageId)+"', '"+str(committerId)+"', '"+str(fileop)+"','"+\
                                             str(filename)+"', '"+datetime+"','"+str(addlines)+"','"+str(dellines)+"', '"+str(commitid)+"', '"+str(fileid_in)+\
                                             "', '"+str(committerId)+"', '"+cdate+"', '"+cdate+"', '"+str(sha)+"', 'git')"
                                   #print 'before sqlexecute'
                                   #1/0
                                   insertsql(sql,cursor)
                         
                                   
                                   

               #is svn, assume commit datetime = message datetime                   
               elif msg_format=='svn':
                    try:
                         rev=subject.split('commit: r')[1].split(' ')[0]
                    except:
                         rev=''
                         
                    while 1:
                         num+=1
                         if num>=mlen-1:#finish phrasing message
                              break
                         elif (body[num-1]=='\n' and num>1) or num==0:

                              if body[num:num+8]=='Removed:':
                                   fileop="rm"
                                   num=num+9
                                   flagg=0
                                   while (body[num]==' '):
                                        ccount=0
                                        while(body[num+ccount]!='\n'):
                                             ccount=ccount+1
                                             
                                        filename=body[num+4:num+ccount]

                                        #assign 0 to removed file in commit record(doesnot change file id in file list)
                                        commitid+=1
                                        sql ="insert into commits"+TABLE_POSTFIX+" (list, messageid, committerid, file_operation,"+\
                                             " file_name, datetime, addlines, dellines,id,file_id,authorid,author_datetime,commit_datetime,sha_or_rev,format) values "+\
                                             "('"+str(listidIn)+"', '"+str(messageId)+"', '"+str(committerId)+"', '"+str(fileop)+"','"+\
                                             str(filename)+"', '"+datetime+"','"+str(0)+"','"+str(0)+"', '"+str(commitid)+"','"+str(0)+\
                                             "', '"+str(committerId)+"', '"+datetime+"', '"+datetime+"', '"+rev+"', 'svn')"
                                        insertsql(sql,cursor)

                                        
                                        #check if the file has been in the file list, if so, mark it as removed
                                        
                                        
                                        if filename[-1]=='/':
                                             sql="update filelist"+TABLE_POSTFIX+" set isremoved=true where position(\'"+str(filename)+"\' in filename)>0"
                                        else:
                                             sql="update filelist"+TABLE_POSTFIX+" set isremoved=true where filename=\'"+str(filename)+"\'"
                                        insertsql(sql,cursor)
                                        num=num+ccount+1

                                        
                              elif body[num:num+7]=='Added:\n':#only check copied folders
                                   num=num+7
                                   while (body[num]==' '):
                                        ccount=0
                                        while(body[num+ccount]!='\n'):
                                             ccount=ccount+1
                                        filename=body[num+4:num+ccount]
                                        
                                        num=num+ccount+1

                                        filepath=[]
                                        if body[num+4:num+19]=="  - copied from" and filename[-1]=='/':
                                             ccount=0
                                             while(body[num+ccount]!='\n'):
                                                  ccount=ccount+1
                                             oldfilename=body[num:num+ccount].split(", ")[-1]
                                             num=num+ccount+1
                                             sql="select filename,fileid from filelist"+TABLE_POSTFIX+" where position(\'"+str(oldfilename)+"\' in filename)>0 "+\
                                                  "and isremoved is False"
                                              
                                             cursor.execute(sql)
                                             filepath=cursor.fetchall()
                                             
                                             for ii in range(len(filepath)):#repath all the files in the folder
                                                  fileid+=1
                                                  sql="insert into filelist"+TABLE_POSTFIX+" (list, filename,fileid) values"+\
                                                       "('"+str(listidIn)+"', '"+str(filename)+str(filepath[ii][0].split(oldfilename)[-1])+"', '"+\
                                                       str(fileid)+"')" #filepath[ii][1])+"')"
                                                  insertsql(sql,cursor)
                                                  
                                        
                                             
                                             
                              elif body[num:num+7]=='Added: ': #add new file
                                   fileop="add"
                                   num=num+7
                                   ccount=0
                                   addlines=0
                                   while(body[num+ccount]!='\n'):
                                        ccount=ccount+1
                                   filename=body[num:num+ccount]#get the name of added file

                                   flag_startcount=0
                                   while num<mlen-1:
                                        num+=1
                                        if body[num-1]=='\n':
                                             if body[num]=="=" and flag_startcount==0:
                                                  flag_startcount=1
                                             elif body[num]=='\n' and body[num+1].isalpha() and flag_startcount:#end of the file
                                                  #num-=1
                                                  break
                                             elif body[num]=='+' and body[num+1]!='+':
                                                  addlines+=1
                                   
                                   #add file into the file list
                                   fileid+=1
                                   sql="insert into filelist"+TABLE_POSTFIX+" (list, filename,fileid) values"+\
                                        "('"+str(listidIn)+"', '"+str(filename)+"', '"+\
                                        str(fileid)+"')"
                                   insertsql(sql,cursor)

                                   #insert one file change into commit list
                                   commitid+=1

                                   sql ="insert into commits"+TABLE_POSTFIX+" (list, messageid, committerid, file_operation,"+\
                                        " file_name, datetime, addlines, dellines,id,file_id,authorid,author_datetime,commit_datetime,sha_or_rev,format) values "+\
                                        "('"+str(listidIn)+"', '"+str(messageId)+"', '"+str(committerId)+"', '"+str(fileop)+"','"+\
                                        str(filename)+"', '"+datetime+"','"+str(addlines)+"','"+str(0)+"', '"+str(commitid)+"','"+str(fileid)+\
                                        "', '"+str(committerId)+"', '"+datetime+"', '"+datetime+"', '"+rev+"', 'svn')"
                                   insertsql(sql,cursor)
                                   
                              elif body[num:num+10]=='Modified: ':
                                   fileop="mod"
                                   num=num+10
                                   ccount=0
                                   addlines=0
                                   dellines=0
                                   while(body[num+ccount]!='\n' and body[num+ccount]!='\r'):
                                        ccount=ccount+1
                                   filename=body[num:num+ccount]#get the name of modified file
                                   

                                   flag_startcount=0
                                   while num<mlen-1:
                                        num+=1
                                        if body[num-1]=='\n':
                                             if body[num]=="=" and flag_startcount==0:
                                                  flag_startcount=1
                                             elif body[num]=='\n' and body[num+1].isalpha() and flag_startcount:#end of the file
                                                  #num-=1
                                                  break
                                             elif body[num]=='+' and body[num+1]!='+':
                                                  addlines+=1
                                             elif body[num]=='-' and body[num+1]!='-':
                                                  dellines+=1


                                   #check if the file has been in the file list
                                   sql="select fileid from filelist"+TABLE_POSTFIX+" where filename='"+str(filename)+"' and isremoved is False"
                                   
                                   cursor.execute(sql)
                                   fileid_in=cursor.fetchone()
                                   if fileid_in is None:
                                   #if not, add it to the file list
                                        
                                        fileid+=1
                                        sql="insert into filelist"+TABLE_POSTFIX+" (list, filename,fileid) values"+\
                                             "('"+str(listidIn)+"', '"+str(filename)+"', '"+\
                                             str(fileid)+"')"
                                        insertsql(sql,cursor)
                                        fileid_in=fileid
                                   else:
                                        fileid_in=fileid_in[0]
                                   
                                   
                                   #insert one file change into commit list
                                   commitid+=1 #numeric id
                                   #sql ="insert into commits"+TABLE_POSTFIX+" (list, messageid, aliasid, file_operation, file_name, datetime, addlines, dellines,id,file_id) values "+\
                                   #     "('"+str(listidIn)+"', '"+str(messageId)+"', '"+str(committerId)+"', '"+str(fileop)+"','"+\
                                   #     str(filename)+"', '"+datetime+"','"+str(addlines)+"','"+str(dellines)+"', '"+str(commitid)+"', '"+str(fileid_in)+"')"
                                   sql ="insert into commits"+TABLE_POSTFIX+" (list, messageid, committerid, file_operation,"+\
                                        " file_name, datetime, addlines, dellines,id,file_id,authorid,author_datetime,commit_datetime,sha_or_rev,format) values "+\
                                        "('"+str(listidIn)+"', '"+str(messageId)+"', '"+str(committerId)+"', '"+str(fileop)+"','"+\
                                        str(filename)+"', '"+datetime+"','"+str(addlines)+"','"+str(dellines)+"', '"+str(commitid)+"','"+str(fileid_in)+\
                                        "', '"+str(committerId)+"', '"+datetime+"', '"+datetime+"', '"+rev+"', 'svn')"
                                   insertsql(sql,cursor)
                                   
                                   #if messageId=='20110821193133.77D112388B56@eris.apache.org':
                                    #    print num
                                     #   print filename
                                   

                                   
                              elif body[num:num+8]=='Copied: ':
                                   fileop="copy"
                                   num=num+8
                                   ccount=0
                                   addlines=0
                                   dellines=0
                                   while(body[num+ccount]!='\n'):
                                        ccount=ccount+1
                                   name_in=body[num:num+ccount]#get the name of copied file
                                   filename=name_in.split(" ")[0]
                                   oldfilename=name_in.split(", ")[-1].split(")")[0]

                                   flag_startcount=0
                                   while num<mlen-1:
                                        num+=1
                                        if body[num-1]=='\n':
                                             if body[num]=="=" and flag_startcount==0:
                                                  flag_startcount=1
                                             elif body[num]=='\n' and body[num+1].isalpha() and flag_startcount:#end of the file
                                                  #num-=1
                                                  break
                                             elif body[num]=='+' and body[num+1]!='+':
                                                  addlines+=1
                                             elif body[num]=='-' and body[num+1]!='-':
                                                  dellines+=1

                                   #check if the file has been in the file list and it still exists in repol; 
                                   #the copied file and the old file are two files (two file ids)
                                   #thus give the copied file a new file id
                                   
                                   '''sql="select fileid from filelist"+TABLE_POSTFIX+" where filename='"+str(filename)+"' and isremoved=False"'''
                                   '''
                                   cursor.execute(sql)
                                   fileid_in=cursor.fetchone()
                                   if fileid_in is None:
                                        #if not, add new name to the file list
                                        '''
                                   fileid+=1
                                   sql="insert into filelist"+TABLE_POSTFIX+" (list, filename,fileid) values"+\
                                        "('"+str(listidIn)+"', '"+str(filename)+"', '"+\
                                        str(fileid)+"')"
                                   insertsql(sql,cursor)
                                   fileid_in=fileid
                                   '''else:
                                        fileid_in=fileid_in[0]'''

                                   #insert one file change into commit list
                                   commitid+=1
                                   #sql ="insert into commits"+TABLE_POSTFIX+" (list, messageid, aliasid, file_operation, file_name, datetime, addlines, dellines,id,file_id) values "+\
                                   #     "('"+str(listidIn)+"', '"+str(messageId)+"', '"+str(committerId)+"', '"+str(fileop)+"','"+\
                                   #     str(filename)+"', '"+datetime+"','"+str(addlines)+"','"+str(dellines)+"', '"+\
                                   #     str(commitid)+"', '"+str(fileid_in)+"')"

                                   sql ="insert into commits"+TABLE_POSTFIX+" (list, messageid, committerid, file_operation,"+\
                                        " file_name, datetime, addlines, dellines,id,file_id,authorid,author_datetime,commit_datetime,sha_or_rev,format) values "+\
                                        "('"+str(listidIn)+"', '"+str(messageId)+"', '"+str(committerId)+"', '"+str(fileop)+"','"+\
                                        str(filename)+"', '"+datetime+"','"+str(addlines)+"','"+str(dellines)+"', '"+str(commitid)+"','"+str(fileid_in)+\
                                        "', '"+str(committerId)+"', '"+datetime+"', '"+datetime+"', '"+rev+"', 'svn')"
                                   insertsql(sql,cursor)
                    #inReplyTo = "NULL"
                    #if "in-reply-to" in message:
                    #     inReplyTo = escape(fixMsgId(message["in-reply-to"]))

                    '''sql = "insert into commits (list, messageid, aliasid, file_operation, datetime, addlines, dellines) values "+\
                         "('"+str(listidIn)+"', '"+str(messageId)+"', '"+str(fileop)+"', '"+str(aliasId)+"','"+\
                         datetime+"','"+str(addlines)+"','"+str(dellines)+"')"
                    #print sql
     #               print "Inserting", messageId
                    
                    try:
                         cursor.execute(sql)
                          
                    except pg8000.errors.ProgrammingError, e:
                         #print message
                         print e
                         print >> errorsLog, e
                         print >> errorsLog, sql'''
          errorsLog.close()
          #break
          
     print str(x+1)+"    "+str(len(PJnameList))+"\r\n"
     conn.commit()
     conn.close()
#Content=getMonthContent(Fpath,pathList[15])



