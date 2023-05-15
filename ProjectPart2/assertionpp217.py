# Hundreds of trips take place each day


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
query = "SELECT DATE(tstamp),COUNT(DISTINCT trip_id) FROM breadcrumb GROUP BY DATE(tstamp);"

# Execute the query
cursor.execute(query)

# Fetch all the rows returned by the query
rows = cursor.fetchall()
print("Number of trips on each date as below:")
for row in rows:
    reading_date = row['date']
    trip_count = row['count']
    print(str(reading_date) + ": " + str(trip_count))

cursor.close()
conn.close()

