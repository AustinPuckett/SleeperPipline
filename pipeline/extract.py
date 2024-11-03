import requests
import json

SLEEPER_URL = r'https://api.sleeper.app/v1'
SLEEPER_ENDPOINT = None # TODO: Make sleeper endpoints global vars
STD_TIMEOUT = 10
LONG_TIMEOUT = 100


def get_league(league_id):
    '''
    Get json data from "Get a specific league" endpoint of the Sleeper API.

    :param league_id: Unique league identifier provided by the Sleeper API.
    :return:
    '''
    url = f'https://api.sleeper.app/v1/league/{league_id}'
    r = requests.get(url, timeout=STD_TIMEOUT)

    json_data = r.json()

    return json_data


def get_rosters(league_id):
    '''
    Get json data from the rosters endpoint of the Sleeper API.

    :param league_id: Unique league identifier provided by the Sleeper API.
    :return:
    '''
    url = f"https://api.sleeper.app/v1/league/{league_id}/rosters"
    r = requests.get(url, timeout=LONG_TIMEOUT)

    json_data = r.json()

    return json_data


def get_user_info(league_id):
    '''
    Get json data from "Getting users in a league" endpoint of the Sleeper API.

    :param league_id: Unique league identifier provided by the Sleeper API.
    :return:
    '''
    url = f"https://api.sleeper.app/v1/league/{league_id}/users"
    r = requests.get(url, timeout=STD_TIMEOUT)

    json_data = r.json()

    return json_data


def get_matchups(week, league_id):
    '''
    Get json data from "Getting matchups in a league" endpoint of the Sleeper API.

    :param week: The week number of the nfl season.
    :param league_id: Unique league identifier provided by the Sleeper API.
    :return:
    '''
    valid_weeks = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18]
    if week in valid_weeks:
        url = f"https://api.sleeper.app/v1/league/{league_id}/matchups/{week}"
        r = requests.get(url, timeout=LONG_TIMEOUT)

        json_data = r.json()
        return json_data

    raise ValueError('Invalid week passed. Week must be an integer greater than or equal to one and less than eighteen.')


def get_transactions(week, league_id):
    '''
    Get json data from "Get transactions" endpoint of the Sleeper API.

    :param week: The week number of the nfl season.
    :param league_id: Unique league identifier provided by the Sleeper API.
    :return:
    '''
    url = f'https://api.sleeper.app/v1/league/{league_id}/transactions/{week}'
    r = requests.get(url, timeout=STD_TIMEOUT*2)

    json_data = r.json()

    return json_data


def get_nfl_state():
    '''
    Get json data from "Get NFL state" endpoint of the Sleeper API.

    :return:
    '''
    url = f"https://api.sleeper.app/v1/state/nfl"
    r = requests.get(url, timeout=STD_TIMEOUT/2)

    json_data = r.json()

    return json_data


def get_draft_info(draft_id):
    '''
    Get json data from "Get a specific draft" endpoint of the Sleeper API.

    :param draft_id: Unique draft identifier provided by the Sleeper API.
    :return:
    '''
    url = f"https://api.sleeper.app/v1/draft/{draft_id}"
    r = requests.get(url, timeout=STD_TIMEOUT)

    json_data = r.json()

    return json_data


def get_draft_picks(draft_id):
    '''
    Get json data from "Get all picks in a draft" endpoint of the Sleeper API.

    :param draft_id: Unique draft identifier provided by the Sleeper API.
    :return:
    '''
    url = f"https://api.sleeper.app/v1/draft/{draft_id}/picks"
    r = requests.get(url, timeout=STD_TIMEOUT)

    json_data = r.json()

    return json_data


def get_player_data():
    '''
    Get json data from the players endpoint of the Sleeper API.

    Only run this API call once per day.

    :return:
    '''
    url = r"https://api.sleeper.app/v1/players/nfl"
    r = requests.get(url, timeout=LONG_TIMEOUT)
    json_data = r.json()

    print(json_data)
    return json_data


def extract_all(api_params={'league_id':None,
                            'draft_id':None,
                            'week': None},
                include_player_data=True):
    '''Call all "get" functions and save their output_data to a specified directory.'''

    num_weeks = 18
    json_dict = {'user_info': get_user_info(league_id=api_params['league_id']),
                 'nfl_state': get_nfl_state(),
                 'rosters': get_rosters(league_id=api_params['league_id']),
                 'draft_info': get_draft_info(draft_id=api_params['draft_id']),
                 'draft_picks': get_draft_picks(draft_id=api_params['draft_id']),
                 'league': get_league(league_id=api_params['league_id']),
                 'transactions': get_transactions(week=api_params['week'], league_id=api_params['league_id']),
                 # 'draft_order': get_draft_order(draft_id=api_params['draft_id'],
                 }

    if include_player_data:
        json_dict['player_data'] = get_player_data()
    else:
        json_dict['player_data'] = None

    for i in range(1, num_weeks + 1):
        json_name = 'matchups_week_' + "{:02d}".format(i)  # Transform relies on this naming convention
        json_dict[json_name] = get_matchups(week=i, league_id=api_params['league_id'])

    return json_dict
