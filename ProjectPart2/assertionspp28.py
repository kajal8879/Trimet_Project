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
query = "SELECT * FROM trip;"

# Execute the query
cursor.execute(query)

# Fetch all the rows returned by the query
rows = cursor.fetchall()
print("Number of rows:", len(rows))
# Process the rows
for row in rows:
    # Access column values using column names
    print(row)
    assert row['service_key'] == 'Weekday' or row['service_key'] == 'Saturday' or row["service_key"] == 'Sunday', "service key should be Weekday', 'Saturday', or 'Sunday' "
    # Do something with the data
cursor.close()
conn.close()

