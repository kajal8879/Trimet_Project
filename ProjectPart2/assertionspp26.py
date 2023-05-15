import psycopg2
from psycopg2 import extras

# Establish the connection to the PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="Qwerty@#23"
)

# Create a cursor object to interact with the database
cursor = conn.cursor(cursor_factory=extras.RealDictCursor)

# Define the query
query = "SELECT * FROM Trip;"

# Execute the query
cursor.execute(query)

# Fetch all the rows returned by the query
rows = cursor.fetchall()
print("Number of rows:", len(rows))
# Process the rows
res={}
for row in rows: 
    event=str(row['trip_id'])
    vid=(row['vehicle_id'])
    
    try:
        if (res[event]!=(vid)):
            print("Vehicel id is unique for trip id ")
        else:
            continue
    except:            
        res[event]=vid
        
    


                

cursor.close()
conn.close()

