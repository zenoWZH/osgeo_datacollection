import os,pg8000
from numpy.lib.utils import source
import unicodedata
import tarfile
import gzip
import email, codecs
import time
import pandas as pd
import numpy as np
import gc
#import logging
from tqdm import tqdm

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

from sqlalchemy import create_engine
from sqlalchemy import types as sqltype
from config.database import HOST, PORT, USER, PASSWORD, DATABASE

psql_engine = create_engine("postgresql://"+USER+":"+PASSWORD+"@"+HOST+":"+str(PORT)+"/"+DATABASE)

def un_gz(file_name):
    f_name = file_name.replace(".gz","")
    g_file = gzip.GzipFile(file_name)
    open(f_name, "wb+").write(g_file.read())
    g_file.close()

    return(f_name)



def escape(s):
    return (s).replace("'", "''").replace('%', '%%').replace('<','').replace('>','')



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



def time_filename(file_csv_route):
    try:
        file_month = file_csv_route.split('/')[-1].split('.')[0]
        #time_format = "%Y-%B"
        #str_time = file_month.split('.')[0]
        #time_struct = time.strptime(str_time, time_format)
        #timeStamp = pd.Timestamp(time_struct)

        timeStamp = pd.Timestamp(file_month)    #pandas powerful!
        return timeStamp
    except BaseException as err:
        print(err)
        return 0

def time_parse(message_date):
    
    try:
        time_format = "%a %b %d %H:%M:%S %Y"
        tsp = time.strptime(message_date, time_format)
    except ValueError as e:
        try:
            time_format =  "%a %d %b %Y %H:%M:%S %z "
            tsp = time.strptime(message_date.split('(')[0].replace(",",""), time_format)
        except ValueError as e:
            try:
                time_format =  "%a %d %b %Y %H:%M:%S %z"
                tsp = time.strptime(message_date.split('(')[0].replace(",",""), time_format)
            except ValueError as e:
                try:
                    time_format =  "%d %b %Y %H:%M:%S %z"
                    tsp = time.strptime(message_date.split('(')[0].replace(",",""), time_format)
                except ValueError as e:
                    try:
                        time_format = "%a %d %b %Y %H:%M:%S"
                        tsp = time.strptime(message_date.split('(')[0][:-5].replace(",",""), time_format)
                    except ValueError as e:
                        try:
                            time_format = "%a %d %b %Y %H:%M "
                            tsp = time.strptime(message_date.split('(')[0][:-5].replace(",",""), time_format)
                        except ValueError as e:
                            try:
                                time_format = "%a %b %d %H:%M:%S %Y"
                                tsp = time.strptime(message_date.split('(')[0][:-5].replace(",",""), time_format)
                            except BaseException as err:
                                    print(err, "Return timestamp as 0")
                                    #flag = True
                                    return 0
    time_format = "%Y-%m-%d %H:%M:%S"
    return(pd.Timestamp(time.strftime(time_format, tsp)))


  

def getMonthContent(path):
    #print(path)
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
        print("Database connect error:%s" % e)

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
        print("Database connect error:%s" % e)
        return
    

    project_id = escape(pjname)
    
    #OLD PART BELOW#
    #    global counter_thread
    #    global counter_message
    #    for message in messages:
    #        counter_message+= 1
    #        if "references" in message:
    #            
    #            thread_id_old = escape(message["references"].split()[-1]+"__"+pjname)
    #            
    #            thread_id = escape(project_id+"#"+str(counter_thread)+"#"+message["references"].split()[-1])
    #
    #            sql_remove = """DELETE FROM thread WHERE thread_id='%s' """ % (thread_id_old)
    #            try:
    #                db.commit()
    #                cursor.execute(sql_remove)
    #                db.commit()
    #            except BaseException as err:
    #                pass
    #            
    #        elif "message-id" in message:
    #            counter_thread+=1
    #            project_id = escape(pjname)
    #            thread_id = escape(project_id+"#"+str(counter_thread)+"#"+message["message-id"].split()[-1])
    #        else:
    #            counter_thread+= 1
    #            thread_id= escape(pjname+"#"+str(counter_thread)+"#")
    #            continue
    #OLD PART ABOVE#
    threads = messages["thread_id"].drop_duplicates().values
    for thread_id in threads:
        
        thread_id = str(thread_id)
        thread_type = "emails"
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
    
    list_person = []
    #list_mailaddress = []
    #list_id = []

    for message in messages:
        if "from" in message:
            personname = email.utils.parseaddr(message["from"])[0]
            mailaddress = message["from"].split('(')[0].replace(' at ','@')
            # NEED REDO with #!!!!!!!!!!!!!!
            list_person.append([escape(personname+'_'+mailaddress), escape(personname), escape(mailaddress)])
            #list_mailaddress.append(escape(mailaddress))
            #list_id.append(escape(personname+'_'+mailaddress))

    df_aliases = pd.DataFrame(list_person).drop_duplicates()

    #add_aliase(escape(personname+'_'+mailaddress), escape(personname), escape(mailaddress), "email")
    for id, name, mailadd in df_aliases.values:
        add_aliase(id, name, mailadd, "emails") 
            

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

def saveMessagetocsv(messages, csv_file, element):
    
    message_id = []
    thread_id = []
    author_aliase_id = []
    author_name = []
    message_text = []
    timestamp = []
    subject = []
    reply_to = []
    references = []
    global counter_thread
    global dict_thread
    global counter_message
    for message in messages:
        #flag = False
        counter_message+= 1
        if "from" in message:
            personname = email.utils.parseaddr(message["from"])[0]
            #personname = asciiCodify(personname)  #不可用
            mailaddress = message["from"].split('(')[0].replace(' at ','@')

            author_aliase_id.append(escape(personname+'_'+mailaddress))
            author_name.append(escape(personname))
        else:
            continue

        try:
            if "message-id" in message:
                message_id.append(escape(element+"#"+str(counter_message)+"#"+message["message-id"]))
            else:
                message_id.append(escape(element+"#"+str(counter_message)+"#"))
        except BaseException as e:
            print(e)
            message_id.append(escape(element+"#"+str(counter_message)+"#"))
        #print(message_id[-1])

        if "references" in message:
            references.append(str(message["references"].split()))
            reference = message["references"].split()[0] ###########
            if reference in dict_thread:
                thread_no = dict_thread[reference]
                thread_id.append(escape(element+"#"+str(thread_no)+"#"+message["references"].split()[0]))
            else:
                counter_thread+= 1
                thread_no = counter_thread
                dict_thread[reference] = thread_no
                thread_id.append(escape(element+"#"+str(thread_no)+"#"+message["references"].split()[0]))
        elif "message-id" in message:
            references.append(np.nan)
            reference = message["message-id"]
            counter_thread+= 1
            thread_no = counter_thread
            dict_thread[reference] = thread_no
            thread_id.append(escape(element+"#"+str(thread_no)+"#"+message["message-id"]))
        else:
            references.append(np.nan)
            counter_thread+= 1
            thread_no = counter_thread
            dict_thread[reference] = thread_no
            thread_id.append(escape(element+"#"+str(thread_no)+"#"))
        

        message_text.append(escape(message.__str__()))

        if "subject" in message:
            subject.append(escape(message["subject"]))
        else:
            subject.append("")

        if 'In-Reply-To' in message:
            reply_to.append(escape(message['In-Reply-To']))
        else:
            reply_to.append("")

        if "date" in message:
            try:
                timestamp.append(pd.Timestamp(message["date"]))
            except BaseException as err:
                #print(err)
                timestamp.append(max(time_filename(csv_file), time_parse(message["date"])))

            
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
    df_message["timestamp" ] = pd.Series(timestamp)#.apply(lambda x: pd.Timestamp(x))
    df_message["message_text" ] = pd.Series(message_text)
    df_message["subject"] = pd.Series(subject)
    df_message["reply_to"] = pd.Series(reply_to)
    df_message["mail_references"] = pd.Series(references)
    
    df_message.to_csv(csv_file)

    #print(message.keys())
    return df_message
    
def dataframe_to_psql(data1):
    
    df_psql = data1.astype(str)
    df_psql['timestamp'] = df_psql['timestamp'].apply(lambda x: x.replace('NaT',"NULL"))
    for col in df_psql.columns.values :
        df_psql[col]= df_psql[col].apply(lambda x : x.encode('utf-8','ignore').decode("utf-8").replace("\x00", "\uFFFD"))
        df_psql[col]= df_psql[col].apply(lambda x : "NULL" if x=='nan' else x)
    df_psql = df_psql.drop(df_psql[df_psql['message_id']=="NULL"].index)
    df_psql.to_sql(name='message', con=psql_engine, if_exists= 'append', index= False, chunksize=1)

    print("Data Saved to psql!")

# Main Function
global counter_thread
global counter_message
global dict_thread
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
    #print(proj_id)
    projs.add(proj_id[0])
conn.commit()
    
df_all = pd.DataFrame(columns =["message_id", "thread_id", "author_aliase_id", "author_name", "receivers_name", "message_text", "timestamp", "subject", "reply_to" ])

#for element in filelist_in[11:12]:
for element in tqdm(filelist_in):
    
    print("Starting Folder:", element)
    counter_thread = 0
    counter_message = 0
    dict_thread = {}
    element_messages = []
    

    # Project name and write the log
    list_out=open("filelist.txt","w+")
    if ("-commit" in element) or ("-dev" in element)or ("-psc" in element)or ("-discuss" in element):
        pjname = element.split("-")[0]
    elif ("_commit" in element) or ("_dev" in element):
        pjname = element.split("_")[0]
    else:
        pjname = element.split('-')[0]
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
    df_proj = pd.DataFrame(columns =["message_id", "thread_id", "author_aliase_id", "author_name", "receivers_name", "message_text", "timestamp", "subject", "reply_to" ])
    txtlist = os.listdir(DIR+"/"+element)

    for fname in txtlist:
        if '.gz' in fname:
            txtname = un_gz(DIR+"/"+element+"/"+fname)
            
    txtlist = os.listdir(DIR+"/"+element)
    for file_month in txtlist:
        
        if not ('.txt' in file_month):
            continue

        if '.gz' in file_month:
            continue
        
        
        #print(file_month)
        messages = getMonthContent(DIR+"/"+element+'/'+file_month)
        #print(len(messages))
        element_messages.extend(messages)
       
        try:
            conn.commit()
            csv_path = DIR_csv+"/"+element+'/'
            if not os.path.exists(csv_path):
                os.makedirs(csv_path)
            csv_file = csv_path+file_month.replace(".txt",'')+".csv"
            #print(csv_file)
            df_thismonth = saveMessagetocsv(messages, csv_file, element)
        except BaseException as e:
            print(e)

        df_proj = df_proj.append(df_thismonth, ignore_index = True).drop_duplicates().dropna(how ='all', axis=0)
    
    checkAliase(conn, element_messages)
    add_thread(df_proj, pjname)

        
    csv_file = csv_path+element+"_all.csv"
    df_all = df_all.append(df_proj, ignore_index = True)
    df_proj.to_csv(csv_file)
    # Drop Timezone!!!
    df_proj["timestamp"] = df_proj["timestamp"].apply(lambda x: pd.Timestamp(x).tz_localize(None)).fillna(np.nan)
    #df_proj["timestamp"] = df_proj["timestamp"].apply(lambda x: '' if pd.isnull(x) else x)

    try:
    #    pass
        # SAVE TO psql HERE!!!!!!!!!!!!
        dataframe_to_psql(df_proj)
    except BaseException as err:
        print(err)
        print(pjname)
        break

    gc.collect()

df_all.to_csv('emails_all.csv')

