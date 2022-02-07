# EffectiveCare
- Import two data files:  Hospital_General_Information.csv and Timely_and_Effective_Care-Hospital.csv
- Clean data: 1) fields of numeric types have valid value, otherwise fill with 0 2) Score field of effective care is a percentage. So value > 100 is converted to 100 3) add "patient count" = "Sample" / "Score" * 100 to show the actual number of patients
- Persist data in Sqlite in a file with two database table: care and hospital. Hospital table takes facility id as primary key and create index with it. Care table uses facility id and measure id as primary key and facility id is also a foreign key.  Data is inserted in batch to increase efficiency.
- Test: not finished yet as unittest doesn't run in Anaconda environment that I setup on my Windows machine for some reason. Run out of time to fix them
