import sqlite3
import random
from datetime import datetime, timedelta

def setup():
    conn = sqlite3.connect('products.db')
    return conn

# Create ratings table
def create_ratings_table(conn):
    try:
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS Ratings 
                (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp DATE, user_id INTEGER, product_id INTEGER, rating INTEGER)''')
        conn.commit()
        print("========== Ratings table created successfully ===========")

    except sqlite3.Error as error:
        print(f"Error while deleting table: {error}")

# utility method to generate timestamp
def generate_timestamp(start_date, end_date):
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    random_seconds = random.randint(0, 86400)
    return start_date + timedelta(days=random_days, seconds=random_seconds)

# Add randomly generated 100000 ratings to the table with specified conditions
def add_ratings(conn):
    c = conn.cursor()
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)
    batch_size = 1000
    ratings = []
    
    for _ in range(100000):
        timestamp = generate_timestamp(start_date, end_date)
        user_id = random.randint(1, 1000)
        product_id = random.randint(1, 1000)
        rating = random.randint(1, 5)
        ratings.append((timestamp, user_id, product_id, rating))
        
        if len(ratings) == batch_size:
            c.executemany("INSERT INTO Ratings (timestamp, user_id, product_id, rating) VALUES (?, ?, ?, ?)", ratings)
            conn.commit()
            ratings = []

    if ratings:
        c.executemany("INSERT INTO Ratings (timestamp, user_id, product_id, rating) VALUES (?, ?, ?, ?)", ratings)
        conn.commit()

    print("========== Products ratings added successfully ===========")

# Create products ratings monthly aggregates table
def create_ratings_monthly_aggregates_table(conn):
    try:
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS RatingsMonthlyAggregates 
                (id INTEGER PRIMARY KEY AUTOINCREMENT, product_id INTEGER, month INTEGER, avg_rating REAL)''')
        conn.commit()
        print("========== Ratings monthly aggregated table created successfully ===========")

    except sqlite3.Error as error:
        print(f"Error while deleting table: {error}")

# Add products ratings monthly aggregates based on ratings table
def add_ratings_monthly_aggregates(conn):
    c = conn.cursor()
    batch_size=1000
    
    c.execute('''
        SELECT 
            product_id,
            strftime('%m', timestamp) AS month,
            AVG(rating) AS avg_rating
        FROM Ratings
        GROUP BY product_id, month
    ''')
    
    rows = c.fetchall()
    
    agg_ratings = []
    for row in rows:
        agg_ratings.append(row)
        if len(agg_ratings) == batch_size:
            c.executemany("INSERT INTO RatingsMonthlyAggregates (product_id, month, avg_rating) VALUES (?, ?, ?)", agg_ratings)
            conn.commit()
            agg_ratings = []
    
    if agg_ratings:
        c.executemany("INSERT INTO RatingsMonthlyAggregates (product_id, month, avg_rating) VALUES (?, ?, ?)", agg_ratings)
        conn.commit()

    print("========== Added products ratings monthly aggregates successfully ===========")

# Find top rated products ratings based on ratings monthly aggregates table
def find_top_rated_products(conn):
    c = conn.cursor()    
    c.execute('''
        SELECT month, product_id, avg_rating
        FROM (
            SELECT month, product_id, avg_rating,
                   ROW_NUMBER() OVER (PARTITION BY month ORDER BY avg_rating DESC) as rank
            FROM RatingsMonthlyAggregates
        )
        WHERE rank <= 3
    ''')
    rows = c.fetchall()
    print("========== Fetched top rated products successfully ===========")
    
    return rows

# utility method to delete tables
def delete_table(conn,table_name):
    try:
        cursor = conn.cursor()
        sql_command = f"DROP TABLE IF EXISTS {table_name}"
        cursor.execute(sql_command)
        conn.commit()        
        print(f"========== Table '{table_name}' deleted successfully ===========")

    except sqlite3.Error as error:
        print(f"Error while deleting table: {error}")

# main method to execute the program as specified in the requirement document
if __name__ == "__main__":

    conn = setup()
    create_ratings_table(conn)
    add_ratings(conn)
    create_ratings_monthly_aggregates_table(conn)
    add_ratings_monthly_aggregates(conn)
    top_rated_products = find_top_rated_products(conn)

    # printing results for quick validation
    print("========== Top rated 3 products in each month ===========")
    for row in top_rated_products:
        print(f"Month: {row[0]}, Product ID: {row[1]}, Average Rating: {round(row[2], 1)}")

    # optional cleanup
    delete_table(conn, 'Ratings')
    delete_table(conn, 'RatingsMonthlyAggregates')

    conn.close()