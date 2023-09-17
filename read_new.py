import pandas as pd
import json
import collections
import itertools
from tqdm import tqdm 

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
df = pd.DataFrame(columns=columns)
df = df.astype(column_data_types)
df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d %H:%M:%S.%f')

# Setup for streaming
file_path = "dawnEventDataStream.txt"
chunk_size = 3000 
i = 0
threshold = 23

# Open the file and read it in chunks
with open(file_path, "r", encoding="utf-8") as file:
    pbar = tqdm(total=3500000)  # Initialize tqdm progress bar

    while True:
        chunk = list(itertools.islice(file, chunk_size))
        if not chunk:
            print("end")
            break

        # Process the chunk of lines as JSON and append to the DataFrame
        chunk_data = [json.loads(line) for line in chunk if line.strip()]
        chunk_data = chunk_data[1:]
        user_id_counts = collections.Counter(item["user_id"] for item in chunk_data)
        if i==0:
            print(user_id_counts)
        filtered_chunk = [item for item in chunk_data if user_id_counts[item["user_id"]] >= threshold or item["user_id"] in df["user_id"].values]

        df_chunk = pd.DataFrame(filtered_chunk, columns=columns)
        df = pd.concat([df, df_chunk], ignore_index=True, join='inner')

        i += 1
        pbar.update(len(chunk))  # Update tqdm progress bar

df.set_index("user_id", inplace=True)
df.to_csv("dawnEventDataStream.csv")
pbar.close()  # Close tqdm progress bar