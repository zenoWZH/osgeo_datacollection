import os,pg8000
from numpy.lib.utils import source
import unicodedata
import tarfile
import gzip
import email, codecs
import time
import modin.pandas as pd
import numpy as np
import gc
import logging

from config.database import HOST, PORT, USER, PASSWORD, DATABASE

import psycopg2

os.chdir("/mnt/data0/proj_osgeo/data")
db_config=open('./db_config')
dbHOST=db_config.readline().split('\"')[1]
dbUSER=db_config.readline().split('\"')[1]
dbPASS=db_config.readline().split('\"')[1]
dbDB=db_config.readline().split('\"')[1]
db_config.close()
conn = pg8000.connect(host=dbHOST,user=dbUSER,password=dbPASS,database=dbDB)
conn.commit()




def read_into_buffer(filename):
    buf = bytearray(os.path.getsize(filename))
    with open(filename, 'rb') as f:
        f.readinto(buf)
    f.close()
    return buf

def readfileline(f):
    
    try:
        return f.readline()
    except Exception as e:
        #print(f.tell())
        f.seek(f.tell()+5, 0)
        #print(f.tell())
        #print("Jump 5")
        return -1
    
def readfile(path, enco):
    try:
        with open(path, 'r', encoding= enco) as f:
            return f.readlines()
    except BaseException as err:
        try:
            x = []
            with open(path, 'r', encoding= enco) as f:
                fline = readfileline(f)
                while fline != "":
                    if fline != -1 :
                        x.append(fline)
                    fline = readfileline(f)
                    #print(fline)
                return x
                    
        except Exception as e:
            print(e)
            return None

def _decode(b, enco):
    try:
        return str(b, encoding= enco)
    except Exception as e:
        return None

def decode_data(b, added_encode=None):
    """
    bytedecoding
    :param bytes:
    :return:
    """
    #def _decode(b, enco):
    #    try:
    #        return str(b, encoding= enco)
    #    except Exception as e:
    #        return None

    encodes = ['utf-8', 'ascii', "base64"]
    if added_encode:
        encodes = [added_encode] + encodes
    for enco in encodes:
        str_data = _decode(b, enco)
        if str_data != None:
            return str_data
    return None
   
 
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
    return (s).replace("'", "''").replace('%', '%%').replace('<','').replace('>','')
    
# Counts number of lines in file
def file_line_number(file):
    for i, l in enumerate(file):
        pass
    return i + 1


def getMonthContent(path):
    print(path)
    messages = []
    messageText=""
    try:
        #fileContentIn = codecs.open(path)
        #global ContentIn
        #ContentIn=[decode_data(x) for x in fileContentIn.readlines()]
        #ContentIn = fileContentIn.readlines()


        encodes = ['utf-8', 'ascii', "base64", 'ANSI', 'GBK', 'utf-16', 'utf-32']
        for enco in encodes:
            ContentIn = readfile(path, enco)
            if ContentIn is not None:
                #print("File read Success:"+path)
                break
        
        #print(ContentIn)
        #fileContentIn.close()
        if ContentIn is None:
            raise ValueError("Unknown File Coding of file:" + path)
        #messages=[]
        
        for i in range(len(ContentIn)):
            if (i+1 < len(ContentIn) and ContentIn[i].startswith("From ") and ContentIn[i+1].startswith("From:")) or (i+1==len(ContentIn)):
                if len(messageText):
                    message = email.message_from_string(messageText)
                    #print( "Message","From", message["from"], "To", message["to"], "ID", message["message-id"], "Thread", message["references"])
                    messages.append(message)
                messageText = ""
            else:
                messageText += ContentIn[i]
        #print ("there are", len(messages), " messages"," ",path)
        #global totalMessages
        #totalMessages = totalMessages + len(messages)
    except BaseException as e:
        print(e)
        
    if len(messages) == 0:
        return [""]
    return messages


def add_aliase(aliase_id, personname, mailaddress, source):

    try:
        db = psycopg2.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=DATABASE)
            #charset='utf8')
        cursor = db.cursor()
    except Exception as e:
        logging.error("Database connect error:%s" % e)

    #aliase_id, mailaddress = aliase.replace('<', ' ').replace('>', ' ').split()
    aliase_id = escape(aliase_id)
    sql_aliase = """INSERT INTO aliase(aliase_id, personname, mailaddress, source)
                                        values('%s', '%s', '%s', '%s')""" % (aliase_id, personname, mailaddress, source)
    try:
        db.commit()
        cursor.execute(sql_aliase)
        db.commit()
    except Exception as err:
        sql_aliase = """UPDATE aliase SET personname='%s', mailaddress='%s', source='%s' WHERE aliase_id='%s' """ % (personname, mailaddress, source, aliase_id)
        
        try:
            db.commit()
            cursor.execute(sql_aliase)
            db.commit()
        except Exception as err:
            print(err)

    db.close()
    return aliase_id

def add_thread(messages, pjname):
    try:
        db = psycopg2.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=DATABASE)
            #charset='utf8')
        cursor = db.cursor()
    except Exception as e:
        logging.error("Database connect error:%s" % e)
        return
    counter = 0
    counter_message = 0
    for message in messages:
        counter_message +=1
        if "references" in message:
            counter+=1
            thread_id_old = escape(message["references"].split()[-1]+"__"+pjname)
            project_id = escape(pjname)
            thread_id = escape(project_id+"#"+str(counter)+"#"+message["references"].split()[-1])

            sql_remove = """DELETE FROM thread WHERE thread_id='%s' """ % (thread_id_old)
            try:
                db.commit()
                cursor.execute(sql_remove)
                db.commit()
            except BaseException as err:
                pass
            
        elif "message-id" in message:
            project_id = escape(pjname)
            thread_id = escape(project_id+"#"+str(counter_message)+"#"+message["message-id"].split()[-1])
        else:
            continue
            
        thread_type = "email"
        thread_name = escape(project_id+thread_id.split('@')[0])
        #aliase_id, mailaddress = aliase.replace('<', ' ').replace('>', ' ').split()
        #thread_id = escape(thread_id)
        
        

        sql_thread = """INSERT INTO thread(thread_id, thread_name, project_id, thread_type)
                                            values('%s', '%s', '%s', '%s')""" % (thread_id, thread_name, project_id, thread_type)

        try:
            db.commit()
            cursor.execute(sql_thread)
            db.commit()
        except Exception as err:
            sql_thread = """UPDATE thread SET thread_name='%s', project_id='%s', thread_type='%s' WHERE thread_id='%s' """ \
                            % (thread_name, project_id, thread_type, thread_id)
            #print(err)
        
            try:
                db.commit()
                cursor.execute(sql_thread)
                db.commit()
            except Exception as err:
                print(err)

    db.close()
    return 0

def checkAliase(conn, messages):
    
    cursor = conn.cursor()
    for message in messages:
        if "from" in message:
            personname = email.utils.parseaddr(message["from"])[0]
            mailaddress = message["from"].split('(')[0].replace(' at ','@')
            add_aliase(escape(personname+'_'+mailaddress), escape(personname), escape(mailaddress), "email")        
            

def checkThread(conn, messages, pjname):
    cursor = conn.cursor()
    
    threads = set()
    cursor.execute("select thread_id, project_id from thread")
    result=cursor.fetchall()
    for thread_id, project_id in result:
        threads.add((thread_id, project_id))
        
    unknownThreads = set()
    for message in messages:
        if "references" in message:
            thread_id = escape(message["references"].split()[-1]+"__"+pjname)
            project_id = escape(pjname)
            if (thread_id, project_id) not in threads:
                if (thread_id, project_id) not in unknownThreads:
                    unknownThreads.add((thread_id, project_id))
        #else:
            #print("No threading")
    
    if len(unknownThreads) > 0:
        #print(unknownThreads)
        sql = "insert into thread (thread_id, thread_name, project_id) values " + \
                ",".join(["('"+ thread_id+"','" + project_id+thread_id.split('@')[0]+"','" + project_id + "')"
                            for thread_id, project_id in unknownThreads]) + \
                " returning thread_id, project_id"
        
        try:
            cursor.execute(sql)
            result=cursor.fetchall()
            for thread_id, project_id in result:
                threads.add((thread_id, project_id))
        except pg8000.ProgrammingError as e:
            #print message
            print ("insert error")
            print (e)
            
        conn.commit()
    
def saveMessagetocsv(messages, csv_file, pjname):
    
    
    message_id = []
    thread_id = []
    author_aliase_id = []
    author_name = []
    message_text = []
    timestamp = []
    counter = 0
    for message in messages:
        #flag = False
        counter+=1
        if "from" in message:
            personname = email.utils.parseaddr(message["from"])[0]
            #personname = asciiCodify(personname)  #不可用
            mailaddress = message["from"].split('(')[0].replace(' at ','@')

            author_aliase_id.append(personname+'_'+mailaddress)
            author_name.append(personname)
        else:
            continue

        try:
            if "message-id" in message:
                message_id.append(escape(pjname+"#"+str(counter)+"#"+message["message-id"]))
        except BaseException as e:
            print(e)
            message_id.append("")
            

        if "references" in message:
            thread_id.append(escape(pjname+"#"+str(counter)+"#"+message["references"].split()[-1]))
        else:
            thread_id.append('')

        message_text.append(escape(message.__str__()))

        if "date" in message:
            try:
                timestamp.append(pd.Timestamp(message["date"]))
            except BaseException as err:
                print(err)
                time_format = "%a %b %d %H:%M:%S %Y"
                try:
                    tsp = time.strptime(message["date"], time_format)
                except ValueError as e:
                    try:
                        time_format =  "%a %d %b %Y %H:%M:%S %z "
                        tsp = time.strptime(message["date"].split('(')[0].replace(",",""), time_format)
                    except ValueError as e:
                        try:
                            time_format =  "%a %d %b %Y %H:%M:%S %z"
                            tsp = time.strptime(message["date"].split('(')[0].replace(",",""), time_format)
                        except ValueError as e:
                            try:
                                time_format =  "%d %b %Y %H:%M:%S %z"
                                tsp = time.strptime(message["date"].split('(')[0].replace(",",""), time_format)
                            except ValueError as e:
                                try:
                                    time_format = "%a %d %b %Y %H:%M:%S"
                                    tsp = time.strptime(message["date"].split('(')[0][:-5].replace(",",""), time_format)
                                except ValueError as e:
                                    try:
                                        time_format = "%a %d %b %Y %H:%M "
                                        tsp = time.strptime(message["date"].split('(')[0][:-5].replace(",",""), time_format)
                                    except BaseException as e:
                                            print(e)
                                            #flag = True
                                            timestamp.append(0)
                                            continue
            
                
                time_format = "%Y-%m-%d %H:%M:%S"
                timestamp.append(pd.Timestamp(time.strftime(time_format, tsp)))
            
        else:
            timestamp.append(0)
            
        conn.commit()
        #sql = "insert into message (message_id, thread_id, author_aliase_id, author_name, message_text, timestamp) values " + \
        #        ",('"+ message_id[-1]+ "','"+ thread_id[-1]+ "','"+ author_aliase_id[-1]+ "','"+ author_name[-1]+ "','"+ '' + "','"+ timestamp[-1]+ "'),"+ \
        #        "returning message_id"
        #print(sql)
        #try:
        #    cursor.execute(sql)
            
        #except pg8000.ProgrammingError as e:
            #print message
        #    print ("insert error")
        #    print (e)
        #    print (sql)
        conn.commit()
        
    df_message = pd.DataFrame(columns =["message_id", "thread_id", "author_aliase_id", "author_name", "receivers_name", "message_text", "timestamp" ])
    df_message["message_id"] = pd.Series(message_id)
    df_message["thread_id"] = pd.Series(thread_id)
    df_message["author_aliase_id"] = pd.Series(author_aliase_id)
    df_message["author_name"] = pd.Series(author_name)
    df_message["timestamp" ] = pd.Series(timestamp).apply(lambda x: pd.Timestamp(x))
    df_message["message_text" ] = pd.Series(message_text)
    
    df_message.to_csv(csv_file)
    
# Main Function

# Unzip the txt files
DIR = './raw'
DIR_csv = './csv'
filelist_in=os.listdir(DIR)


################################Check Project Exist

cursor = conn.cursor()
conn.commit()

projs = set()
cursor.execute("select proj_id from project")
result=cursor.fetchall()
for proj_id in result:
    print(proj_id)
    projs.add(proj_id[0])
conn.commit()
    
#for element in filelist_in[:1]:
for element in filelist_in:
    element_messages = []
    # Project name and write the log
    list_out=open("filelist.txt","w+")
    if ("-commit" in element) or ("-dev" in element):
        pjname = element.split("-")[0]
    elif ("_commit" in element) or ("_dev" in element):
        pjname = element.split("_")[0]
    else:
        pjname = element.split()[0]
    list_out.write(DIR+"/"+element+"\n")
    list_out.close()
    
    if pjname not in projs:
        sql = "insert into project (proj_id) values " + "('"+ escape(pjname) + "')" 
        
        cursor.execute(sql)
        conn.commit()
        projs.add(pjname)
        
    # UNZIP ALL .gz files, run once
    #gzlist = os.listdir(DIR+"/"+element)

    #for f_gz in gzlist:
    #    f_gz_dir = DIR+"/"+element+"/"+f_gz
    #    print(f_gz_dir)
    #    if not os.path.exists(DIR+"/"+element+"/"+"txtfile"):
    #        os.makedirs(DIR+"/"+element+"/"+"txtfile")
    #    command = "gunzip "+f_gz_dir+' '+DIR+"/"+element+"/txtfile/"
    #    print(command)
    #    os.system(command)
 
    txtlist = os.listdir(DIR+"/"+element)
    for file_month in txtlist:
        
        messages = getMonthContent(DIR+"/"+element+'/'+file_month)
        #print(len(messages))
        
        try:
            conn.commit()
            checkAliase(conn, messages)
            add_thread(messages, pjname)
            csv_path = DIR_csv+"/"+element+'/'
            if not os.path.exists(csv_path):
                os.makedirs(csv_path)
            csv_file = csv_path+file_month.replace(".txt",'')+".csv"
            saveMessagetocsv( messages, csv_file, pjname)
        except BaseException as e:
            print(e)
        element_messages.extend(messages)
    csv_file = csv_path+element+"_all.csv"    
    saveMessagetocsv( element_messages, csv_file, pjname)
    
        
        
        
        
        
        
        
        
        
        #Only Superuser can do
        #conn.commit()
        #sql = "COPY message(message_id, thread_id, author_aliase_id, author_name, message_text, timestamp) FROM" + \
        #        "'"+ csv_file+ "' DELIMITER ',' CSV HEADER" 
        #cursor.execute(sql)
        #conn.commit()
    gc.collect()
