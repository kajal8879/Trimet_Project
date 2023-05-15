import psycopg2

# Establish the connection to the PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="Ganeshgani24!"
)

# Create a cursor object to interact with the database
cursor = conn.cursor()

# Define the query to count the number of trips with more than 50% of vehicles under 15mph
query = """
SELECT COUNT(*)
FROM (
    SELECT trip_id
    FROM breadcrumb
    WHERE speed < 15
    GROUP BY trip_id
    HAVING COUNT(*) * 2 > (
        SELECT COUNT(*)
        FROM trip
        WHERE trip.trip_id = breadcrumb.trip_id
    )
) subquery;
"""

# Execute the query
cursor.execute(query)

# Fetch the result
result = cursor.fetchone()

# Print the result
print("Number of trips with more than 50% of vehicles under 15mph:", result[0])

# Close the cursor and the connection
cursor.close()
conn.close()

