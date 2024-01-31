from itertools import zip_longest
import gspread
import csv
import pandas as pd
import logs.logging_config as l_config

def load_to_df(file: str, sheet: str):
    logger = l_config.etl_logging("logs/logs_extract.log")
    logger.info(f"Started loading table {sheet}")
    # get sheets credentials
    sheet_credential = gspread.service_account()

    # open the needed spreadsheet
    spreadsheet = sheet_credential.open(file)
    logger.info("Connected to the spreadsheet")
    
    # open the needed worksheet 
    table = spreadsheet.worksheet(table)
    logger.info("Connected to the worksheet")

    values = table.get_all_values()

    df = pd.DataFrame(values[1:],columns=values[0])

    return df