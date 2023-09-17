import pandas as pd
columns = [
    'user_id', 'time', 'platform', 'is_beginner', 'level', 'country_code',
    'game_server', 'fps', 'position_x', 'position_y', 'position_z',
    'session_id', 'oneSecondAggregatedEventCounts'
]
df = pd.DataFrame(columns = columns)
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
my = ['sample1.csv', 'sample2.csv', 'sample3.csv']
jon = ['sample4.csv', 'sample5.csv', 'sample6.csv']
for f in my:
    df = pd.concat([df, pd.read_csv(f, dtype=column_data_types)], ignore_index=True)
for f in jon:
    df = pd.concat([df, pd.read_csv(f, usecols=columns, dtype=column_data_types)], ignore_index=True)
df.set_index("user_id", inplace=True)
df.drop_duplicates(inplace=True)
df.to_csv('all_sample.csv')