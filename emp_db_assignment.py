import csv
import os
import psycopg2
username = os.getenv('username')
port = os.getenv('port')
hostname = os.getenv('hostname')
dbname = os.getenv('dbname')
password=os.getenv('password')

directory = "E:\\program_8\\database_assign"
files = os.listdir(directory)
csv_files = []
for file in files:
    if file.endswith('.csv'):
        csv_files.append(file)
#print(csv_files)
def write_data_into_db(sql_query):
    try:
        connection = 0
        connection = psycopg2.connect(user=username,
                                      password=password,
                                      port=port,
                                      host=hostname,
                                      database=dbname)
        cursor = connection.cursor()
        cursor.execute(sql_query)
        connection.commit()
        print("record added...!")
    except Exception as e:
        print("Error while writing in db ",e)
    finally:
        if connection:
            cursor.close()
            connection.close()

for csv_file in csv_files:
    path = directory+"\\"+csv_file
    with open(path,'r') as file_obj:
        csv_reader = csv.reader(file_obj)
        header = next(csv_reader)
        #print(header)
        for row in csv_reader:
            #print(row)
            if csv_file == "data_science_team.csv":
                sql_query = f"""
                        insert into myschema.data_science values
                        ('{row[0]}','{row[1]}','{row[2]}','{row[3]}','{row[4]}',
                        '{row[5]}','{row[6]}','{row[7]}','{row[8]}');"""
                #write_data_into_db(sql_query)
            if csv_file == "emp_record_table.csv":
                sql_query = f"""
                        insert into myschema.emp_record values
                        ('{row[0]}','{row[1]}','{row[2]}','{row[3]}','{row[4]}',
                        '{row[5]}','{row[6]}','{row[7]}','{row[8]}','{row[9]}',
                        {row[10]},'{row[11]}');"""
                #write_data_into_db(sql_query)
            if csv_file == "proj_table.csv":
                sql_query = f"""
                        insert into myschema.project values
                        ('{row[0]}','{row[1]}','{row[2]}','{row[3]}','{row[4]}',
                        '{row[5]}','{row[6]}');"""
                #write_data_into_db(sql_query)

def read_data_from_db(sql_query):
    all_records = []
    try:
        connection = 0
        connection = psycopg2.connect(user=username,
                                      password=password,
                                      port=port,
                                      host=hostname,
                                      database=dbname)
        cursor = connection.cursor()
        cursor.execute(sql_query)
        all_records = cursor.fetchall()
        return all_records
    except Exception as e:
        print("Error while writing in db ", e)
    finally:
        if connection:
            cursor.close()
            connection.close()
# sql_query = "select *from myschema.emp_record"
# sql_query = "select *from myschema.data_science"
# sql_query = "select *from myschema.project"
# sql_query3 = "select emp_id,first_name,last_name,gender,dept from myschema.emp_record "
# sql_query41 = """select emp_id,first_name,last_name,gender,dept,emp_rating from myschema.emp_record
#                  where emp_rating < 2"""
# sql_query42 = """select emp_id,first_name,last_name,gender,dept,emp_rating from myschema.emp_record
#                  where emp_rating > 4"""
sql_query43 = """select emp_id,first_name,last_name,gender,dept,emp_rating from myschema.emp_record
                 where emp_rating between 2 and 4"""
sql_query5 = """select concat(first_name,' ',last_name)as name,dept
                from myschema.emp_record where dept = 'FINANCE' """
records = read_data_from_db(sql_query5)

print(len(records))
for record in records:
    print(record)