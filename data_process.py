import logging
import sqlite3
from sqlite3 import Error
import yaml
import pandas as pd
import numpy as np
import sys
from dbsync import DBSync

class DataProcessor:
    def __init__(self, config_file):
        self.config_file = config_file
        try:
            with open(self.config_file, 'r') as f:
                self.config = yaml.safe_load(f)
            
            if 'hospital_file' not in self.config or 'care_file' not in self.config or 'db_file' not in self.config:
                sys.exit(1)
            if not self.config['hospital_file'].endswith(".csv") or not self.config['care_file'].endswith(".csv"):
                sys.exit(1)
        except Exception as e:
            logging.error(str(e))
            sys.exit(1)
        
    
    def clean_data(self):
        df_h = pd.read_csv(self.config['hospital_file'])
        
        #make sure facility id exists as it is primary key for hospital

        #make sure all numeric columns having valid values, fill with 0 for non numerical values
        df_h['Hospital overall rating'] = pd.to_numeric(df_h['Hospital overall rating'], errors='coerce').fillna(0)                                        
        df_h['Hospital overall rating footnote'] = pd.to_numeric(df_h['Hospital overall rating footnote'], errors='coerce').fillna(0)  
        df_h['MORT Group Measure Count'] = pd.to_numeric(df_h['Hospital overall rating footnote'], errors='coerce').fillna(0)                            
        df_h['Count of Facility MORT Measures'] = pd.to_numeric(df_h['Hospital overall rating footnote'], errors='coerce').fillna(0)                         
        df_h['Count of MORT Measures Better'] = pd.to_numeric(df_h['Hospital overall rating footnote'], errors='coerce').fillna(0)                        
        df_h['Count of MORT Measures No Different'] = pd.to_numeric(df_h['Hospital overall rating footnote'], errors='coerce').fillna(0)                   
        df_h['Count of MORT Measures Worse'] = pd.to_numeric(df_h['Hospital overall rating footnote'], errors='coerce').fillna(0)                            
        df_h['MORT Group Footnote']  = pd.to_numeric(df_h['Hospital overall rating footnote'], errors='coerce').fillna(0)                                   
        df_h['Safety Group Measure Count']  = pd.to_numeric(df_h['Hospital overall rating footnote'], errors='coerce').fillna(0)                             
        df_h['Count of Facility Safety Measures']  = pd.to_numeric(df_h['Hospital overall rating footnote'], errors='coerce').fillna(0)                      
        df_h['Count of Safety Measures Better']  = pd.to_numeric(df_h['Hospital overall rating footnote'], errors='coerce').fillna(0)                       
        df_h['Count of Safety Measures No Different']   = pd.to_numeric(df_h['Hospital overall rating footnote'], errors='coerce').fillna(0)                
        df_h['Count of Safety Measures Worse']  = pd.to_numeric(df_h['Hospital overall rating footnote'], errors='coerce').fillna(0)                         
        df_h['Safety Group Footnote'] = pd.to_numeric(df_h['Hospital overall rating footnote'], errors='coerce').fillna(0)                           
        df_h['READM Group Measure Count']  = pd.to_numeric(df_h['Hospital overall rating footnote'], errors='coerce').fillna(0)                              
        df_h['Count of Facility READM Measures']   = pd.to_numeric(df_h['Hospital overall rating footnote'], errors='coerce').fillna(0)                      
        df_h['Count of READM Measures Better']    = pd.to_numeric(df_h['Hospital overall rating footnote'], errors='coerce').fillna(0)                       
        df_h['Count of READM Measures No Different']  = pd.to_numeric(df_h['Hospital overall rating footnote'], errors='coerce').fillna(0)                  
        df_h['Count of READM Measures Worse']  = pd.to_numeric(df_h['Hospital overall rating footnote'], errors='coerce').fillna(0)                       
        df_h['READM Group Footnote'] = pd.to_numeric(df_h['Hospital overall rating footnote'], errors='coerce').fillna(0)                                  
        df_h['Pt Exp Group Measure Count']  = pd.to_numeric(df_h['Hospital overall rating footnote'], errors='coerce').fillna(0)                             
        df_h['Count of Facility Pt Exp Measures'] = pd.to_numeric(df_h['Hospital overall rating footnote'], errors='coerce').fillna(0)                       
        df_h['Pt Exp Group Footnote'] = pd.to_numeric(df_h['Hospital overall rating footnote'], errors='coerce').fillna(0)                                 
        df_h['TE Group Measure Count']  = pd.to_numeric(df_h['Hospital overall rating footnote'], errors='coerce').fillna(0)                                
        df_h['Count of Facility TE Measures']  = pd.to_numeric(df_h['Hospital overall rating footnote'], errors='coerce').fillna(0)                          
        df_h['TE Group Footnote'] = pd.to_numeric(df_h['Hospital overall rating footnote'], errors='coerce').fillna(0) 
        df_h['ZIP Code'] = pd.to_numeric(df_h['ZIP Code'], errors='coerce').fillna(0)

        
        #facility id and measure id should both non null for care 
        df_c = pd.read_csv(self.config['care_file'])
        
        #make sure all numeric columns having valid values, fill with 0 for non numerical values
        df_c['ZIP Code'] = pd.to_numeric(df_c['ZIP Code'], errors='coerce').fillna(0)
        df_c['Score'] = pd.to_numeric(df_c['Score'], errors='coerce').fillna(0)
        df_c['Footnote'] = pd.to_numeric(df_c['Footnote'], errors='coerce').fillna(0)
        df_c['Sample'] = pd.to_numeric(df_c['Sample'], errors='coerce').fillna(0)


        # Since Score is the percentage of sample, anything over 100 is converted to 100
        df_c['Score'] = df_c['Score'].apply(lambda x: 100.0 if x>=100 else x)
        #Actual patients = sample / score
        df_c['Patient Count'] = df_c['Sample']/df_c['Score'] * 100
        df_c['Patient Count'].fillna(0)

        return (df_h, df_c)

    def process_data(self):
        db = DBSync(self.config['db_file'])
        conn = db.create_db()
        (df_h, df_c) = self.clean_data()

        h_recs = df_h.to_records(index=False)
        total_count = 0
        data_recs = []
        rec_count = 0
        for rec in h_recs:
            data_recs.append(rec)
            rec_count += 1
            total_count += 1
            if len(data_recs) % 10000 == 0:
                db.batch_insert_hospital(conn, data_recs)
                logging.info("Hospital rec insert {} records, total_records".format(rec_count, total_count))
                rec_count = 0
                data_recs = []
        if len(data_recs) > 0:
            db.batch_insert_hospital(conn, data_recs)
            data_recs = []
            logging.info("Hospital rec insert {} records, total_records".format(rec_count, total_count))

        c_recs = df_c.to_records(index=False)
        total_count = 0
        data_recs = []
        rec_count = 0
        for rec in c_recs:
            data_recs.append(rec)
            rec_count += 1
            total_count += 1
            if len(data_recs) % 10000 == 0:
                db.batch_insert_care(conn, data_recs)
                logging.info("Care rec insert {} records, total_records".format(rec_count, total_count))
                rec_count = 0
                data_recs = []
        if len(data_recs) > 0:
            db.batch_insert_care(conn, data_recs)
            data_recs = []
            logging.info("Care rec insert {} records, total_records".format(rec_count, total_count))   
        
if __name__ == '__main__':
    processor = DataProcessor("config.yml")
    processor.process_data()
    


