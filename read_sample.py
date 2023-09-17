import pandas as pd
import json
import collections
import itertools
from tqdm import tqdm 
import random
# Create an empty DataFrame with the desired columns
columns = [
    'user_id', 'time', 'platform', 'is_beginner', 'level', 'country_code',
    'game_server', 'fps', 'position_x', 'position_y', 'position_z',
    'session_id', 'oneSecondAggregatedEventCounts'
]
column_data_types = {
    'user_id': int,
    'time': str,
    'platform': str,
    'is_beginner': bool,
    'level': str,
    'country_code': str,
    'game_server': str,
    'fps': float,
    'position_x': float,
    'position_y': float,
    'position_z': float,
    'session_id': str,
    'oneSecondAggregatedEventCounts': object,
}
start = 30000
end = 65201
num_samples = 1000  # Change this to the number of random integers you want to sample

random_integers = set(random.randint(start, end) for _ in range(num_samples))

print(random_integers)

df = pd.DataFrame(columns=columns)
df = df.astype(column_data_types)
df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d %H:%M:%S.%f')

# Setup for streaming
file_path = "dawnEventDataStream.txt"
chunk_size = 10000 
i = 0
threshold = 90

# Open the file and read it in chunks
with open(file_path, "r", encoding="utf-8") as file:
    pbar = tqdm(total=10000000)  # Initialize tqdm progress bar

    while i<12000000//chunk_size:
        chunk = list(itertools.islice(file, chunk_size))
        if not chunk:
            print("end")
            break
        if i<2000000//chunk_size:
            pass
        else:
            # Process the chunk of lines as JSON and append to the DataFrame
            chunk_data = [json.loads(line) for line in chunk if json.loads(line)["user_id"] in random_integers]
            chunk_data = chunk_data[1:]
            #user_id_counts = collections.Counter(item["user_id"] for item in chunk_data)
            #if i==0:
            #    print(user_id_counts)
            #if df["user_id"].nunique()>40:
                #filtered_chunk = [item for item in chunk_data if item["user_id"] in df["user_id"].values]
            #else:
                #filtered_chunk = [item for item in chunk_data if user_id_counts[item["user_id"]] >= threshold or item["user_id"] in df["user_id"].values]
            #filtered_chunk = [item for item in chunk_data if item["user_id"] in random_integers]
            filtered_chunk = chunk_data
            df_chunk = pd.DataFrame(filtered_chunk, columns=columns)
            df = pd.concat([df, df_chunk], ignore_index=True, join='inner')

        i += 1
        pbar.update(len(chunk))  # Update tqdm progress bar

df.set_index("user_id", inplace=True)
df.to_csv("sample3.csv")
pbar.close()  # Close tqdm progress bar