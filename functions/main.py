# Welcome to Cloud Functions for Firebase for Python!
# To get started, simply uncomment the below code or create your own.
# Deploy with `firebase deploy`

from firebase_functions import https_fn
from firebase_admin import initialize_app
import requests
import json
import tempfile

initialize_app()


@https_fn.on_request()
def get_teams(req: https_fn.Request) -> https_fn.Response:
    import pandas as pd
    def process_endpoint_url(endpoint_url, pop_key=None):
        """
        Fetches data from a URL, parses JSON, and optionally pops a key.

        Args:
          endpoint_url: The URL to fetch data from.
          pop_key: The key to pop from the JSON data (optional, defaults to None).

        Returns:
          A pandas DataFrame containing the processed data
        """
        json_result = requests.get(endpoint_url).content

        data = json.loads(json_result)

        # if pop_key is provided, pop key and normalize nested fields
        if pop_key:
            df_result = pd.json_normalize(data.pop(pop_key), sep='_')
        # if pop_key is not provided, normalize entire json
        else:
            df_result = pd.json_normalize(data)

        return df_result
    teams_endpoint_url = 'https://statsapi.mlb.com/api/v1/teams?sportId=1'

    teams = process_endpoint_url(teams_endpoint_url, 'teams')
    keys = ['id', 'league_id', 'teamCode', 'teamName', 'league_name']
    teams = teams[keys]
    teams['logo_link'] = teams['id'].apply(lambda x: f'https://www.mlbstatic.com/team-logos/{x}.svg')
    return https_fn.Response(teams.to_json())

@https_fn.on_request()
def query_player(req: https_fn.Request) -> https_fn.Response:
    from firebase_admin import storage
    import pickle as pkl
    playerId = req.args.get('playerId')
    blob = storage.bucket('gs://mlb-project-2.firebasestorage.app').blob('data_cleaned.pkl')
    with tempfile.NamedTemporaryFile() as temp:
        blob.download_to_filename(temp.name)
        with open(temp.name, 'rb') as f:
            player_hitting = pkl.load(f)
    if playerId in player_hitting.keys():
        return https_fn.Response(json.dumps(player_hitting[playerId]))
    else:
        return https_fn.Response('Player not found', status=404)

@https_fn.on_request()
def store_hr(req: https_fn.Request) -> https_fn.Response:
    from firebase_admin import storage
    import pandas as pd

    bucket = storage.bucket('gs://mlb-project-2.firebasestorage.app')
    blob = bucket.blob('data_cleaned.pkl')

    def process_endpoint_url(endpoint_url, pop_key=None):
        """
        Fetches data from a URL, parses JSON, and optionally pops a key.

        Args:
          endpoint_url: The URL to fetch data from.
          pop_key: The key to pop from the JSON data (optional, defaults to None).

        Returns:
          A pandas DataFrame containing the processed data
        """
        json_result = requests.get(endpoint_url).content

        data = json.loads(json_result)

        # if pop_key is provided, pop key and normalize nested fields
        if pop_key:
            df_result = pd.json_normalize(data.pop(pop_key), sep='_')
        # if pop_key is not provided, normalize entire json
        else:
            df_result = pd.json_normalize(data)

        return df_result
    season = 2024  # @param {type:"integer"}

    # Can change season to get other seasons' games info
    schedule_endpoint_url = f'https://statsapi.mlb.com/api/v1/schedule?sportId=1&season={season}'

    schedule_dates = process_endpoint_url(schedule_endpoint_url, "dates")

    games = pd.json_normalize(
        schedule_dates.explode('games').reset_index(drop=True)['games'])
    player_hitting = dict()
    for i in range(len(games['gamePk'])):
        game_pk = games['gamePk'].iloc[i]
        single_game_feed_url = f'https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live'
        print(game_pk)

        single_game_info_json = json.loads(requests.get(single_game_feed_url).content)
        for j in range(len(single_game_info_json['liveData']['plays']['allPlays'])):
            id = single_game_info_json['liveData']['plays']['allPlays'][j]['matchup']['batter']['id']
            if not (id in player_hitting.keys()):
                player_hitting[id] = []
            hit = []
            for k in range(len(single_game_info_json['liveData']['plays']['allPlays'][j]['playEvents'])):
                if 'isInPlay' in single_game_info_json['liveData']['plays']['allPlays'][j]['playEvents'][k][
                    'details'].keys() and \
                        single_game_info_json['liveData']['plays']['allPlays'][j]['playEvents'][k]['details'][
                            'isInPlay']:
                    hit.append((single_game_info_json['liveData']['plays']['allPlays'][j]['playEvents'][k]['playId'],
                                single_game_info_json['liveData']['plays']['allPlays'][j]['playEvents'][k]['hitData']))
            player_hitting[id] += hit
    for i in player_hitting.keys():
        a = []
        for j in player_hitting[i]:
            if len(j[1]) != 4:
                a.append(j)
        player_hitting[i] = a
    keys = list(player_hitting.keys())
    for i in keys:
        if player_hitting[i] == []:
            player_hitting.pop(i)
    import pickle as pkl
    import io

    # 将对象 pickle 到内存中的 BytesIO 对象
    with io.BytesIO() as in_memory_file:
        pkl.dump(player_hitting, in_memory_file)
        in_memory_file.seek(0)  # 将文件指针重置到开头
        # ... (下一步将 in_memory_file 的内容上传)
        blob.upload_from_file(in_memory_file)