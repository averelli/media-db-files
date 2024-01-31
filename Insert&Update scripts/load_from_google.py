import pandas as pd
import logging
from datetime import datetime
import logs.logging_config as l_config
from gsheets_connect import  load_to_df
from sqlalchemy import create_engine
from database.db_config import DB_NAME, USER, PASSWORD, HOST, PORT


def insert_from_df(df, table):
    """ Inserts data from a DataFrame into the database

        Params:
            df (pandas.DataFrame): a DataFrame to insert the data from
            table (str): name of the table into wich to insert
    """

    schema, table = table.split(".")
    engine = create_engine(f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}")

    df.to_sql(table, con=engine, schema=schema, if_exists="append", index=False)


def process_csv():
    #load csv, convert the dates
    df = load_to_df("Media","Media")
    df = df.rename(columns={"Media name" : "title","Media Type" : "m_type", "Range start" : "r_start", "Range end" : "r_end", "Episode Length" : "ep_length", "Date": "date", "Language" : "language", "Season" : "season"})
    df["date"] = pd.to_datetime(df["date"], format="%d.%m.%Y")
    df["title"] = df["title"].str.strip().str.lower()

    return (df.loc[df["m_type"] == "film", ["date", "title", "ep_length", "language"]].reset_index(drop=True), df[df["m_type"] == "show"].reset_index(drop=True))
    

def unpack_rows(df):
    #create an empty df to add unpacked rows to 
    columns = ['date', 'title', 'episode_num', 'length', 'season', 'language']
    unpacked_df = pd.DataFrame(columns=columns)

    # iterate over each row
    for row in df.itertuples():
        # for each episode will be a row
        watched = int(row.r_end) - int(row.r_start) + 1
        curr_ep = int(row.r_start)
        for i in range(int(watched)):
            unpacked_row = {
                "date" : row.date,
                "title" : row.title,
                "episode_num": curr_ep,
                "length": int(row.ep_length),
                "language": row.language,
                "season": int(row.season)
            }
            curr_ep += 1
            unpacked_df = pd.concat([unpacked_df, pd.DataFrame(unpacked_row, index=[1])], ignore_index=True)
    
    return unpacked_df


def get_ep_table(unpacked_df, shows_df):
    # get the correct length for the rewatched episodes (the latest entry is the correct one)
    episodes = unpacked_df.loc[unpacked_df.groupby(["title", "season", "episode_num"])["date"].idxmax()]

    # assing an id
    episodes["episode_id"] = range(1, len(episodes) +1)

    # join the shows df to get show ids
    episodes = episodes.merge(shows_df, on="title")

    # convert some nums to ints
    episodes[["episode_num", "season"]] = episodes[["episode_num", "season"]].astype(int)
    
    # save only the needed columns
    episodes = episodes[["episode_id", "show_id", "episode_num", "season", "length"]]

    insert_from_df(episodes,"shows.episode")

    return episodes


def get_shows_data(df):
    # create an empty df
    shows = pd.DataFrame(columns=["show_id", "title"])

    # select the unique titles
    shows["title"] = df["title"].drop_duplicates().reset_index(drop=True)
    
    # create id
    shows["show_id"] = range(1, len(shows) + 1)

    insert_from_df(shows,"shows.show")

    return shows



def create_shows_log(unpacked_df, shows_df, ep_df):
    # merge to get show_id
    res = unpacked_df.merge(shows_df, on="title")
    
    # merge to get episode_id
    res = res.merge(ep_df, on=["show_id", "season", "episode_num"])

    # sort by date to organize
    res.sort_values(by="date", inplace=True)

    # save columns
    res = res[["date", "episode_id", "language"]]

    insert_from_df(res,"shows.shows_log")


def get_films_info(df):
    # create a separate films table
    films = pd.DataFrame(columns=["film_id", "title", "length"])

    # delete unnecessary columns from the original df
    df.drop(["date", "language"], axis=1)

    # set all the titles into lowercase
    df["title"] = df["title"].str.lower()

    # get only unique titles
    df = df.drop_duplicates(subset=["title"])

    # copy data to the new df
    films[["title", "length"]] = df[["title", "ep_length"]]

    # create an id for films
    films["film_id"] = range(1, len(films) + 1)

    insert_from_df(films,"films.film")

    return films
    

def create_films_log(df, films):
    films_log = df.merge(films, on="title")

    # select needed columns
    films_log = films_log[["date", "film_id", "language"]]

    insert_from_df(films_log, "films.films_log")


def load_reading_log():
    df = load_to_df("reading","books_log")
    
    df.columns = df.columns.str.lower()

    df["date"] = df["date"].apply(lambda x: datetime.strptime(x,"%d.%m.%Y"))
    
    insert_from_df(df[["date","book_id","status"]], "reading.books_log")

def load_books():
    df = load_to_df("reading","Books")

    df.columns = df.columns.str.lower()

    insert_from_df(df,"reading.book")


def main():

    # setup and config logger
    logger = l_config.etl_logging("logs/logs_extract.log")
    
    logger.info("STARTING DATA PROCESSING")

    # separate into films and shows
    df_films, df_shows = process_csv()
    logger.info("Data separated into shows and films")

    # PROCESSING SHOWS

    # unpackedten the table so each row contains info about a single episode watched
    unpacked_df = unpack_rows(df_shows)

    # create a table with all the shows
    shows_df = get_shows_data(unpacked_df)
    logger.info("Shows data processed")

    # create a table with all the episodes data
    episodes_df = get_ep_table(unpacked_df, shows_df)
    logger.info("Episodes data processed")

    # create a daily log table with all the right ids
    create_shows_log(unpacked_df, shows_df, episodes_df)
    logger.info("Shows log data processed")

    # PROCESS FILMS

    # create a table with films 
    films = get_films_info(df_films)
    logger.info("Created films table")

    # create a film log
    create_films_log(df_films, films)
    logger.info("Created films daily log")

    #load reading data 
    logger.info("Started inserting reading data")
    load_books()
    load_reading_log()
    logger.info("Finished inserting reading data")

    logger.info("DATA PROCESSING COMPLETED")


if __name__ == '__main__':
    main()