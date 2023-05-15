# There cannot exist a trip without a vehicle id


import psycopg2
from psycopg2 import extras

# Establish the connection to the PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="your_password"
)

# Create a cursor object to interact with the database
cursor = conn.cursor(cursor_factory=extras.RealDictCursor)

# Define the query
query = "SELECT count(*) AS count FROM trip WHERE trip_id IS NOT NULL AND vehicle_id IS NULL;"

# Execute the query
cursor.execute(query)

# Fetch all the rows returned by the query
rows = cursor.fetchall()

for row in rows:
    count = row['count']
    print("Number of trips with no vehicle_id: " + str(count))
    assert (row['count']==0),"vehicle_id should not be null"

cursor.close()
conn.close()

