import logging
import sqlite3
from sqlite3 import Error
import yaml
import pandas as pd
import numpy as np
import sys

class DBSync:
    def __init__(self, db_file):
        self.db_file = db_file
        
    
    def query_table(self, conn, sql):
        cur = conn.cursor()
        cur.execute(sql)
        res = []
        for row in cur:
            res.append(row)
        return res
    
    def create_connection(self):
    
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            return conn
        except Error as e:
            logging.error(str(e))

        return conn
            
    def create_db(self):
        # create a database connection
        conn = self.create_connection()
        
        sql_create_hospital_table = '''create table hospital(
                                        facility_id text PRIMARY KEY,         
                                        facility_name text,                                              
                                        address text,                                    
                                        city text,
                                        state text,
                                        zip_code integer,  
                                        county_name text,
                                        phone_number text,
                                        hospital_type text,
                                        hospital_ownership text, 
                                        emergency_services text,
                                        meets_criteria_for_promoting_interoperability_of_ehrs text,
                                        hospital_overall_rating integer,
                                        hospital_overall_rating_footnote real, 
                                        mort_group_measure_count integer,                             
                                        count_of_facility_mort_measures integer, 
                                        count_of_mort_measures_better integer,  
                                        count_of_more_measures_no_different integer,
                                        count_of_mort_measures_worse integer,
                                        mort_group_footnote real,                                  
                                        safety_group_measure_count integer,
                                        count_of_facility_safety_measures integer,                                                    
                                        count_of_safety_measures_better integer,                       
                                        count_of_safety_measures_no_different integer,  
                                        count_of_safety_measures_worse integer,                        
                                        safety_group_footnote real,       
                                        readm_group_measure_count integer, 
                                        count_of_facility_readm_measures integer, 
                                        count_of_readm_measures_better integer,   
                                        count_of_readm_measures_no_different integer,   
                                        count_of_readm_measures_worse integer,                          
                                        readm_group_footnote real,                                  
                                        pt_exp_group_measure_count integer,                             
                                        count_of_facility_pt_exp_measures integer,                      
                                        pt_exp_group_footnote real,                               
                                        te_group_measure_count integer,                           
                                        count_of_facility_te_measures integer,                       
                                        te_group_footnote  real                                    
                                    )               
                                    '''
        sql_create_care_table = '''create table care(
                                        facility_id text, 
                                        facility_name text,
                                        address       text, 
                                        city          text, 
                                        state         text, 
                                        zip_code      integer,  
                                        county_name   text, 
                                        phone_number  text, 
                                        condition     text, 
                                        measure_id    text, 
                                        measure_name  text, 
                                        score         real,
                                        sample        integer,
                                        footnote       real, 
                                        start_date    text, 
                                        end_date      text, 
                                        patient_count  integer,
                                        FOREIGN KEY (facility_id) REFERENCES hospital (facility_id),
                                        PRIMARY KEY (facility_id, measure_id)                                
                                )
                                '''
        # create tables
        if conn is not None:
            # create projects table
            conn.execute(sql_create_hospital_table)
            conn.execute('create unique index uidx1 on hospital(facility_id)')
            conn.commit()

            # create tasks table
            conn.execute(sql_create_care_table)
            conn.execute('create unique index uidx2 on care(facility_id, measure_id)')

            conn.commit()
        else:
            logging.error("Error! cannot create the database connection.")
    
        return conn

    def batch_insert_hospital(self, conn, recs):
            sql = '''replace into hospital(
                                        facility_id,         
                                        facility_name,                                              
                                        address,                                    
                                        city,
                                        state,

                                        zip_code,  
                                        county_name,
                                        phone_number,
                                        hospital_type,
                                        hospital_ownership, 

                                        emergency_services,
                                        meets_criteria_for_promoting_interoperability_of_ehrs,
                                        hospital_overall_rating,
                                        hospital_overall_rating_footnote, 
                                        mort_group_measure_count, 

                                        count_of_facility_mort_measures, 
                                        count_of_mort_measures_better,  
                                        count_of_more_measures_no_different,
                                        count_of_mort_measures_worse,
                                        mort_group_footnote,    

                                        safety_group_measure_count,
                                        count_of_facility_safety_measures,                                                    
                                        count_of_safety_measures_better,                       
                                        count_of_safety_measures_no_different,  
                                        count_of_safety_measures_worse,

                                        safety_group_footnote,       
                                        readm_group_measure_count, 
                                        count_of_facility_readm_measures, 
                                        count_of_readm_measures_better,   
                                        count_of_readm_measures_no_different, 

                                        count_of_readm_measures_worse,                          
                                        readm_group_footnote,                                  
                                        pt_exp_group_measure_count,                             
                                        count_of_facility_pt_exp_measures,                      
                                        pt_exp_group_footnote,      

                                        te_group_measure_count ,                           
                                        count_of_facility_te_measures,                       
                                        te_group_footnote
                                        )
                                    values(?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?)
            '''
            conn.executemany(sql, recs)
            conn.commit()

    def batch_insert_care(self, conn, recs):
            sql = '''replace into care(
                                        facility_id, 
                                        facility_name,
                                        address, 
                                        city, 
                                        state, 

                                        zip_code,  
                                        county_name, 
                                        phone_number, 
                                        condition, 
                                        measure_id, 

                                        measure_name, 
                                        score,
                                        sample,
                                        footnote,
                                        start_date, 

                                        end_date,
                                        patient_count
                                        
                ) values (?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?)
                '''
            conn.executemany(sql, recs)
            conn.commit()

if __name__ == '__main__':
    db_file = 'caredata7.db'
    if len(sys.argv) == 2:
        print("usage: {} <db_file>".format(sys.argv[0]))
        db_file = sys.argv[1]
        

    db = DBSync(db_file)
    conn = db.create_connection()
    sql1= "select count(*) from hospital;"
    print(db.query_table(conn, sql1))
    #OP_31 and score>=50, what are the hospital ratings
    sql2 = "select h.facility_name, h.hospital_overall_rating from hospital h, care c where c.measure_id='OP_31' and c.score>=50 and h.facility_id=c.facility_id"
    print(db.query_table(conn, sql2))
    #OP_22 count by state
    sql3 = "select h.state, count(*) from hospital h, care c where c.measure_id='OP_22' and h.facility_id=c.facility_id group by h.state"
    print(db.query_table(conn, sql3))
    conn.close()
