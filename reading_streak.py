from database.connect_db import send_query
import pandas as pd

# select the needed data to calculate the streak into a df
df = pd.DataFrame(send_query("SELECT date, book_id FROM reading.books_log ORDER BY date asc"))
df.columns = ["date", "book_id"]

# delta column to keep the difference between the dates
delta_col = []

# streak col to later insert into the table
streak_col = []
streak_val = 0

# calculate for every row
for i in range(len(df)):
    # calculate the difference in dates, if it's the first row then set to 1
    val = abs(df.iloc[i]["date"] - df.iloc[i-1]["date"]).days if i != 0 else 1

    delta_col.append(val)

    # if the difference is one day, increase the streak by one, if 0 then it's the same day, else restart the streak from 1
    if val == 1:
        streak_val += 1
    elif val == 0:
        pass
    else:
        streak_val = 1

    # insert streak into the database
    send_query("UPDATE reading.books_log SET streak = %s WHERE date = %s and book_id = %s", (int(streak_val), df.iloc[i]["date"], int(df.iloc[i]["book_id"])))
    