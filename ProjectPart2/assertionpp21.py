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
    assert isinstance(row['speed'], float), "speed should be a float."
    assert row['speed'] >= 0, "speed should be non-negative."
    # Do something with the data
cursor.close()
conn.close()

