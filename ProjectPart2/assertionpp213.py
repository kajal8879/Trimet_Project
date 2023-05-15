import psycopg2
from psycopg2 import extras

# Establish the connection to the PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="Ganeshgani24!"
)

# Create a cursor object to interact with the database
cursor = conn.cursor(cursor_factory=extras.RealDictCursor)

# Define the query
query = "SELECT vehicle_id,trip_id FROM trip;"

# Execute the query
cursor.execute(query)

# Fetch all the rows returned by the query
rows = cursor.fetchall()
#print(rows)

for row in rows:
    vehicle_id = row['vehicle_id']
    trip_id = row['trip_id']
    if vehicle_id is None or vehicle_id == '':
        print('Missing vehicle id')
        break
    elif trip_id is None or trip_id == '':
        print('Missing trip id')
        break
else:
    print('Every vehicle has vehicle id and trip id')

    # Do something with the data
cursor.close()
conn.close()
