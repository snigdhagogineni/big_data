import requests
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2 import sql

# PostgreSQL Configuration
db_config = {
    "dbname": "bbc_news",           # Replace with your database name
    "user": "snigdhagogineni",        # Replace with your PostgreSQL username
    "password": "your_password",    # Replace with your PostgreSQL password
    "host": "localhost",            # Replace with your PostgreSQL host (or AWS RDS endpoint)
    "port": 5432                    # Replace with your PostgreSQL port (default is 5432)
}

# RSS Feed URL
rss_url = 'https://feeds.bbci.co.uk/news/rss.xml'

# Fetch the RSS feed
response = requests.get(rss_url)
response.raise_for_status()  # Ensure the request was successful

# Parse the XML content
root = ET.fromstring(response.content)

# Extract news items
items = root.findall('.//item')

# Prepare the data for PostgreSQL insertion
rss_data = []
for item in items:
    headline = item.find('title').text
    pub_date = item.find('pubDate').text
    rss_data.append((headline, pub_date))

# Insert data into PostgreSQL
try:
    # Connect to PostgreSQL
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Create the `rss_feed` table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS rss_feed (
        id SERIAL PRIMARY KEY,
        headline TEXT NOT NULL,
        publication_date TIMESTAMP NOT NULL
    );
    """
    cursor.execute(create_table_query)

    # Insert data into the table
    insert_query = """
    INSERT INTO rss_feed (headline, publication_date)
    VALUES (%s, %s);
    """
    cursor.executemany(insert_query, rss_data)

    # Commit changes and close the connection
    conn.commit()
    print(f"Inserted {len(rss_data)} records into the database.")

except Exception as e:
    print(f"Error: {e}")

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
