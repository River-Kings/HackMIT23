import os
import pandas as pd
import json
file_path = 'dawnEventDataStream.txt'
num_lines = 100  # Number of lines to retrieve from the bottom
columns = [
    'id', 'user_id', 'time', 'platform', 'is_beginner', 'level', 'country_code',
    'game_server', 'fps', 'position_x', 'position_y', 'position_z',
    'session_id', 'oneSecondAggregatedEventCounts'
]
with open(file_path, 'rb') as f:
    try:  # Catch OSError in case of a one-line file
        f.seek(-2, os.SEEK_END)
        while f.read(1) != b'\n':
            f.seek(-2, os.SEEK_CUR)
    except OSError:
        f.seek(0)

    last_lines = []
    for _ in range(num_lines):
        line = [json.loads(line) for line in _ if line.strip()]
        if not line:
            break  # Reached the beginning of the file
        last_lines.append(line.decode())

# Print the last 100 lines in reverse order (from the bottom up)
pd.DataFrame(last_lines, columns = columns).to_csv("dawn_last100.csv")
#last_lines.to_csv("dawn_last100.csv")
