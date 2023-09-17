import pandas as pd
import json
import numpy as np
df=pd.read_csv('all_sample.csv')
df.rename(columns={'oneSecondAggregatedEventCounts':'events'}, inplace=True)
df['events'].astype(str)
social_interactions = ['player_interaction', 'was_charged_by_player', 'player_lift', 'hand_held', 'healed_player', 
                       'chat_msg', 'milestone_made_5_friends', 'social_feed_impression']
pings = ['ping']
#df['social'] = np.where(any([x in df['events'] for x in social_interactions]), 1, 0)
df['social'] = df['events'].str.contains('|'.join(social_interactions), case=False, regex=True).astype(int)
df['movement'] = df['events'].str.contains('|'.join(pings), case=False, regex=True).astype(int)
df['time'] = pd.to_datetime(df['time'])
agg_funcs = {
    'time': lambda x: x.max()-x.min(),
    'platform': 'first',
    'country_code': 'first',
    'game_server': 'nunique',
    'fps': ['mean', 'std', lambda x: x.max()-x.min()],
    'session_id': 'nunique',
    'social': 'sum',
    'movement': 'sum',
    }

result = df.groupby('user_id').agg(agg_funcs).reset_index()

result.columns = [f'{col[0]}_{col[1]}' if col[1] else col[0] for col in result.columns]
result['time_<lambda>'] = result['time_<lambda>'].dt.total_seconds()
result.to_csv('aggregated.csv', index=False)