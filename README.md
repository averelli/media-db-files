# media-db-files
Repo to keep scripts I use for the media bot database
- DDL scripts folder: keeps the definition of schemas and tables inside the database
- reading_streak.py: used to calculate reading streak for existing data, filling the 
- load_from_google.py: a script used to extract existing data from the Google Sheets and then clean, transform and load it into the database
- gsheet_connect.py: a script to connect to the Google Sheets and extract data from there

Note: some files have incorrect imports inside this repository because they have been used in different parts of the project and now are here just for storage and documentation with no intent of using them from here.