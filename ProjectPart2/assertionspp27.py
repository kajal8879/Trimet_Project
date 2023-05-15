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
query = "SELECT * FROM breadcrumb;"

# Execute the query
cursor.execute(query)

# Fetch all the rows returned by the query
rows = cursor.fetchall()
print("Number of rows:", len(rows))
# Process the rows
for row in rows:
    # Access column values using column names
    print(row)
    assert isinstance(row['vehicle_id',int]), "vehicle_id should be a int"
    assert row['vehicle_id'] > 0 and len(row['vehicleind'])== 4 , "vehicle_id should be be greater than zero"
    # Do something with the data
cursor.close()
conn.close()

