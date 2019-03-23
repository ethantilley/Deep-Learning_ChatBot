import sqlite3
import pandas as pd

timeframes = ['2015-01']

for timeframe in timeframes:
    # Establishing a connection to the SQL sorted and cleaned data.
    connection = sqlite3.connect('../../../Data To Ignore/Data/SQL Table/{}.db'.format(timeframe))
    c = connection.cursor()
    limit = 5000
    last_unix = 0
    cur_length = limit
    counter = 0
    test_done = False
    # Running through the sql file and pairing parent comments (questions/statemenst -> responces) 
    while cur_length == limit:
        df = pd.read_sql("SELECT * FROM parent_reply WHERE unix > {} AND parent NOT NULL AND score > 0 ORDER BY unix ASC LIMIT {}".format(last_unix, limit), connection)
        last_unix = df.tail(1)['unix'].values[0]
        cur_length = len(df)
        # Making test files to pair a small amount of data first before making the larger files
        if not test_done:
            with open("../../../Data To Ignore/Data/Paired Data/test.from", 'a', encoding='utf8') as f:
                for content in df['parent'].values:
                    f.write(content+'\n')
            with open("../../../Data To Ignore/Data/Paired Data/test.to", 'a', encoding='utf8') as t:
                for content in df['comment'].values:
                    t.write(content+'\n')

            cur_length += 300000
            limit += 300000
            test_done = True

        else:
            with open("../../../Data To Ignore/Data/Paired Data/train.from", 'a', encoding='utf8') as f:
                for content in df['parent'].values:
                    f.write(content+'\n')
            with open("../../../Data To Ignore/Data/Paired Data/train.to", 'a', encoding='utf8') as t:
                for content in df['comment'].values:
                    t.write(content+'\n')

        counter += 1
        if counter % 20 == 0:
            print(counter*limit, ' rows completed so far')
