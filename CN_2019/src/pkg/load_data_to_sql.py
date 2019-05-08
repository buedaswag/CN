import pandas as pd
from collections import deque
from sqlalchemy import create_engine

#log memory
import psutil
a = psutil.virtual_memory()
print(a.free)

dir1 = './combined_data_1.txt'

#df1 = pd.read_csv(dir1, header=None, names=['user_id', 'rating', 'year'], 
#usecols=[0, 1, 2])
#load just half of the file
df1 = pd.read_csv(dir1, header=None, names=['user_id', 'rating', 'year'], \
    usecols=[0, 1, 2], nrows=12029131)
df1['rating'] = df1['rating'].astype(float)
print(df1.shape)

df = df1
df_raw =df

# Find empty rows to slice dataframe for each movie
tmp_movies = df_raw[df_raw['rating'].isna()]['user_id'].reset_index()
movie_indices = [[index, int(movie[:-1])] \
    for index, movie in tmp_movies.values]

# Shift the movie_indices by one to get start and endpoints of all movies
shifted_movie_indices = deque(movie_indices)
shifted_movie_indices.rotate(-1)


# Gather all dataframes
user_data = []

# Iterate over all movies
for [df_id_1, movie_id], [df_id_2, next_movie_id] in \
    zip(movie_indices, shifted_movie_indices):
    
    # Check if it is the last movie in the file
    if df_id_1<df_id_2:
        tmp_df = df_raw.loc[df_id_1+1:df_id_2-1].copy()
    else:
        tmp_df = df_raw.loc[df_id_1+1:].copy()
        
    # Create movie_id column
    tmp_df['movie_id'] = movie_id
    
    # Append dataframe to list
    user_data.append(tmp_df)

# Combine all dataframes
df = pd.concat(user_data)
del user_data, df_raw, tmp_movies, tmp_df, shifted_movie_indices, \
    movie_indices, df_id_1, movie_id, df_id_2, next_movie_id
print('Shape User-Ratings:\t{}'.format(df.shape))
print(df.sample(5))
df.shape

df.shape

df.memory_usage(index=True).sum()

a = psutil.virtual_memory()
print(a)

engine = create_engine("mysql+pymysql://root:red@localhost/netflix_db")


#I suspect chucksize is the number of records
df.to_sql(con=engine, name='netflix_data', if_exists='replace', \
    index=False, chunksize=1000)
print('Done.')