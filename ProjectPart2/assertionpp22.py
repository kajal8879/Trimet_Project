import psycopg2
from psycopg2 import extras
from collections import defaultdict

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
query1 = "SELECT * FROM breadcrumb;"
query2 = "SELECT * FROM trip;"

# Execute the query
cursor.execute(query1)

# Fetch all the rows returned by the query
breadcrumbsrows = cursor.fetchall()
cursor.execute(query2)
triprows = cursor.fetchall()
print("Number of rows:", len(breadcrumbsrows))
trip_ids = set(record['trip_id'] for record in triprows)

breadcrumb_trip_id = set(record['trip_id'] for record in breadcrumbsrows)

if breadcrumb_trip_id in trip_ids:
    print("Breadcrumb trip ID is present in trip data.")
else:
    print("Breadcrumb trip ID is not present in trip data.")

cursor.close()
conn.close()

