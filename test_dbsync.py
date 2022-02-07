from dbsync import DBSync
import unittest
import logging

class Test_DBSync(unittest.TestCase):
    def test_create_table(self):
        db = DBSync('testdb')
        conn = db.create_db()
        self.assertTrue(conn != None)
        #verify hospital and care table are created in the database
        sql = "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY 1;"
        res = db.query_table(conn, sql)
        
    def test_batch_insert(self):
        #insert one record to care and hospital table

        #retrive the record back and verify

        

if __name__ == '__main__':
    unittest.main()
