import pandas as pd
import json
import itertools

# Create an empty DataFrame with the desired columns
columns = [
    'id', 'user_id', 'time', 'platform', 'is_beginner', 'level', 'country_code',
    'game_server', 'fps', 'position_x', 'position_y', 'position_z',
    'session_id', 'oneSecondAggregatedEventCounts'
]
column_data_types = {
    'id': int,
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
# Specify the path to the text file
file_path = "dawnEventDataStream.txt"

# Chunk size for reading and appending data
chunk_size = 1000  # Adjust as needed
specific_user_id = 11  # Replace this with the specific user_id you're interested in
a=5

fruits = ["apple", "banana", "cherry"]
x=0



# Open the file and read it in chunks
with open(file_path, "r", encoding="utf-8") as file:
    while x < 5000 :
        chunk = [next(file) for _ in range(chunk_size)]
        x = x+1
    chunk = [next(file) for _ in range(chunk_size)]

    while a>0:
        # Read a chunk of lines
        chunk = [next(file) for _ in range(chunk_size)]

        # If the chunk is empty, we've reached the end of the file
        if not chunk:
            break

        # Process the chunk of lines as JSON and append to the DataFrame
        chunk_data = [json.loads(line) for line in chunk if line.strip()]
        df_chunk = pd.DataFrame(chunk_data, columns=columns)
        df = pd.concat([df, df_chunk], ignore_index=True)
        a=a-1
# Set the "id" column as the index
df.set_index("id", inplace=True)
df.to_csv("playerNewChunkSet1.csv")
# Now, you have a pandas DataFrame with the data stored as specified, and it's memory-efficient for large files.
# You can use this DataFrame for data analysis and manipulation.