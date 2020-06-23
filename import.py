import psycopg2
import os
import sys
from datetime import datetime, timedelta

path = '/tmp/csp_chatlog'
# file_path = '/tmp/csp_chatlog/chatlog-20200305.csv'
schema_name = 'chatlog'
dbname = 'postgres'
# table_name = 'chatlog.logs_20200305'

host = 'localhost'
port = '5432'
user = 'postgres'
pwd = 'cmhk123'

def loop_date(path):

    file_list = []
    for filename in os.listdir(path):
        if filename.endswith(".csv"): 
         # print(os.path.join(directory, filename))
            file_list.append(filename)
    
    return file_list

def pg_load_table(file_path, schema_name, table_name, dbname, host, port, user, pwd):
    start_date = datetime.strptime(table_name.split("_")[1], '%Y%m%d').date()
    end_date = start_date + timedelta(days=1)

    try:
        conn = psycopg2.connect(dbname=dbname, host=host, port=port,\
         user=user, password=pwd)
        #print("Connecting to Database")
        
        create_partition_sql = "CREATE TABLE if not exists " + schema_name + "." + table_name + " (" + \
	        " CONSTRAINT " + table_name + "_date_time_check CHECK (((date_time >= '" + start_date.strftime('%Y-%m-%d') + "'::text) AND (date_time < '" + end_date.strftime('%Y-%m-%d') + "'::text)))" + \
            ") INHERITS (chatlog.logs); "
        
        #print(create_partition_sql)
        cur = conn.cursor()
        cur.execute(create_partition_sql)
        conn.commit()

        f = open(file_path, "r")
        
        # Truncate the table first
        # cur.execute("Truncate {} Cascade;".format(table_name))
        # print("Truncated {}".format(table_name))
        
        # Load table from the file with header
        cur.copy_expert("copy {}.{} from STDIN CSV HEADER QUOTE '\"'".format(schema_name, table_name), f)
        conn.commit()
        #print("Loaded data into {}.{}".format(schema_name, table_name))

        f.close()
        conn.close()
        #print("DB connection closed.")

    except Exception as e:
        print("Error: {}".format(str(e)))
        #sys.exit(1)
    
def replaceChar(file_path):
    with open(file_path) as f:
        newText=f.read().replace('\x00', '')

    with open(file_path, "w") as f:
        f.write(newText)

if __name__ == '__main__':

    file_list = loop_date(path)


    
    for i in file_list:
        replaceChar(os.path.join(path, i))
        table_name = "logs_" + i.split("-")[1].split(".")[0]
        pg_load_table(os.path.join(path, i), schema_name, table_name, dbname, host, port, user, pwd)

    #pg_load_table(file_path, table_name, dbname, host, port, user, pwd)
