import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''open song file, extract relevant columns, 
    insert into song and artist tables '''
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].values.tolist()
    song_data = list(song_data[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values.tolist()
    artist_data = list(artist_data[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''open log file, extract relevant columns and rows,
    insert into songplay, user, and time tables'''
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong'].reset_index()

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    df['hour'] = df['ts'].dt.hour.astype(int)
    df['day'] = df['ts'].dt.day.astype(int)
    df['week'] = df['ts'].dt.week.astype(int)
    df['month'] = df['ts'].dt.month.astype(int)
    df['year'] = df['ts'].dt.year.astype(int)
    df['weekday'] = df['ts'].dt.dayofweek.astype(int)
    
    # create a list with time data and column lables  
    time_data = df[['ts', 'hour', 'day', 'week', 'month', 'year', 'weekday']].values.tolist() 
    column_labels = ['ts', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    
    # create empty dictionary with column labels as keys
    time_dict = {k:[] for k in column_labels}

    # combine column_labels and time_data values into the dict
    for row in time_data[1:]:
        for k,v in zip(column_labels, row):
            time_dict[k].append(v) 
            
    # convert time_dict into a dataframe        
    time_df = pd.DataFrame.from_dict(time_dict)
    
    # insert time data
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
    #-------------end time data-------------#
    
    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]
    
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
    #-------------end user data-------------#

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        row.ts = pd.to_datetime(row.ts, unit='ms')    
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)   
        
        cur.execute(songplay_table_insert, songplay_data)
    #-------------end songplay data-------------#


def process_data(cur, conn, filepath, func):
    '''get all .json files in directory, display a file count,
    iterate over all files and process'''
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    '''main program'''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()