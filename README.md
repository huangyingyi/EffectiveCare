# EffectiveCare
- Import two data files:  Hospital_General_Information.csv and Timely_and_Effective_Care-Hospital.csv
- Clean data:  
  - fields of numeric types have valid value, otherwise fill with 0 
  - Score field of effective care is a percentage. So value > 100 is converted to 100 
  -  add "patient count" = "Sample" / "Score" * 100 to show the actual number of patients
- Persist data in Sqlite in a file with two database table: 
  -  Hospital table takes facility id as primary key and create index with it. Most fields except primary are NULL able to enable schema evolution.
  -  Care table uses facility id and measure id as primary key and facility id is also a foreign key. Most fields except primary are NULL able to enable schema evolution.
  -  Data is inserted in batch to increase efficiency.
- Program Structure
  - data_process.py : main program that takes config.yml as config file, clean data and insert into sqlite db.
  - config.yml: specify input CSV and output database file
  - datasync.py: class definition to work with sqlite 
  - test_dbsync.py: unit test for DBSync class.  Test not finished yet as unittest doesn't run in Anaconda environment that I setup on my Windows machine for some reason. Run out of time to fix them
  - caredata.db: sqlite db file output from running data_process
- To-do
  - Finish and add more unit tests
  - ZIP Code is int64 from the original data source, but it is not inserted correctly in the sqlite table even after calling to_numeric()
