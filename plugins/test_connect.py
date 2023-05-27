# import psycopg2
# import 
# def connect(self):
#         conn = None
#         try:
#             conn = psycopg2.connect(
#                         user="postgres",
#                         password="root",
#                         host="localhost",
#                         port="5432",
#                         database="airflow"
#                     )
#             cursor = conn.cursor()
#         except (Exception, psycopg2.Error) as error:
#             print("Failed to insert record into mobile table", error)
#         finally:
#             if conn is not None:
#                 return conn, cursor

searchfile = open("/home/nhattrieu/airflow/airflow.cfg")
for line in searchfile:
    if ('sql_alchemy_conn' and 'postgresql') in line: 
        print(line)
# print(f.read()) 