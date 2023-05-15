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
query1 = "SELECT * FROM breadcrumb;"

# Execute the query
cursor.execute(query1)

# Fetch all the rows returned by the query
breadcrumbsrows = cursor.fetchall()
print("Number of rows:", len(breadcrumbsrows))

for row in breadcrumbsrows:
    # Access column values using column names
    print(row)
    assert isinstance(row['latitude'], float), "latitude should be a float."
    assert -90 <= row['latitude'] <= 90, "Invalid latitude value."
    # Do something with the data


cursor.close()
conn.close()

