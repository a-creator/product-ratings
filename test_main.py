import unittest
import sqlite3
from main import create_ratings_table, add_ratings, create_ratings_monthly_aggregates_table, add_ratings_monthly_aggregates, find_top_rated_products, delete_table

class TestMain(unittest.TestCase):

    def setUp(self):
        self.db_name = 'test_products.db'
        self.conn = sqlite3.connect(self.db_name)
        create_ratings_table(self.conn)
        create_ratings_monthly_aggregates_table(self.conn)

    def tearDown(self):
        self.conn.close()
        import os
        os.remove(self.db_name)

    def test_create_ratings_table(self):
        c = self.conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Ratings'")
        result = c.fetchone()
        self.assertEqual(result[0], 'Ratings')

    def test_add_ratings(self):
        add_ratings(self.conn)
        c = self.conn.cursor()
        c.execute("SELECT COUNT(*) FROM Ratings")
        result = c.fetchone()
        self.assertEqual(result[0], 100000)

    def test_create_ratings_monthly_aggregates_table(self):
        c = self.conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='RatingsMonthlyAggregates'")
        result = c.fetchone()
        self.assertEqual(result[0], 'RatingsMonthlyAggregates')

    def test_add_ratings_monthly_aggregates(self):
        add_ratings(self.conn)
        create_ratings_monthly_aggregates_table(self.conn)
        add_ratings_monthly_aggregates(self.conn)
        c = self.conn.cursor()
        c.execute("SELECT COUNT(*) FROM RatingsMonthlyAggregates")
        result = c.fetchone()
        self.assertGreater(result[0], 36)

    def test_find_top_rated_products(self):
        add_ratings(self.conn)
        create_ratings_monthly_aggregates_table(self.conn)
        add_ratings_monthly_aggregates(self.conn)
        top_rated_products = find_top_rated_products(self.conn)
        self.assertEqual(len(top_rated_products), 36)

    def test_delete_table(self):
        create_ratings_table(self.conn)
        create_ratings_monthly_aggregates_table(self.conn)
        delete_table(self.conn, 'Ratings')
        delete_table(self.conn, 'RatingsMonthlyAggregates')
        c = self.conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Ratings'")
        result = c.fetchone()
        self.assertIsNone(result)
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='RatingsMonthlyAggregates'")
        result = c.fetchone()
        self.assertIsNone(result)

if __name__ == '__main__':
    unittest.main()