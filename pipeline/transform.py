import json, os
import numpy as np


def get_json_data_from_file(filename,
                            directory):
    '''
    This function returns a json object from the target file.

    :param filename:
    :param directory:
    :return:
    '''

    filepath = os.path.join(directory, filename)
    with open(filepath, "r") as readfile:
        json_data = json.load(readfile)
    return json_data


def transform_first_layer(json_data):
    '''
    This function extracts key:value pairs that do not contain a list, dict, or tuple in the first layer of json_data
    
    :param json_data:
    :return:
    '''
    
    # Create dictionary that contains all key:value pairs that have no depth.
    keys = []
    values = []
    for key, value in json_data.items():
        if has_depth(value) == False:
            keys.append(key)
            try:
                values.append(json_data[key])
            except:
                values.append(None)

    data_dict = dict(zip(keys, values))

    return data_dict


def has_depth(object):
    '''
    This function determine whether or not an object is a list, tuple, or dictionary
    
    :return:
    '''
    try:
        object + 1
        return False
    except:
        try:
            object + 'a'
            return False
        except:
            return object != None


def set_uniqueness(listA, listB):
    '''
    This function return all elements of listA that do not appear in listB.

    :param listA:
    :param listB:
    :return:
    '''
    unique_elements = []
    for element in listA:
        if element not in listB:
            unique_elements.append(element)

    return unique_elements


def dict_to_list(dict_or_list):
    '''
    This function converts a non index-able object to a list
    
    :param dict_or_list:
    :return: [{}, ...] -like object
    '''
    try:
        dict_or_list[0] # TODO: Filters out empty lists and dictionaries with 0 as a key!!! 
        return dict_or_list
    except:
        return [dict_or_list]


def get_json_keys(table_entries):
    '''
    This function gets all unique keys contained within a json object.
    
    TODO: This algorith may be terribly ineffecient.
    
    :param table_entries: Json data object
    :return:
    '''
    table_entries = dict_to_list(table_entries)
    keys = []
    for table_entry in table_entries:
        entry_keys = list(table_entry.keys())
        keys = keys + entry_keys
    keys = np.unique(keys)
    return keys


def convert_dict_to_table_row(entry_dict, table_column_names):
    '''
    This function takes the elements table_column_names and finds the corresponding values in the data_dict.
    
    TODO: This algorith may be terribly ineffecient.

    :param entry_dict:
    :param table_column_names:
    :return row_vals: List of values to be entered into a database table row
    '''

    # Create table entry list: row_vals.
    row_vals = []
    for column_name in table_column_names:
        row_val = None
        for key, value in entry_dict.items():
            if key == column_name:
                if has_depth(value):
                    row_val = repr(value)
                else:
                    row_val = value
        row_vals.append(row_val)

    return row_vals


# Table data preparation, grouped by filename

# league
def transform_league(json_data, year):
    '''
    This function loops through the dictionary in the league.json file. If a value in the dictionary has depth, then it 
    will be excluded from the extraction.
    
    Data is sourced from the "league.json" file which is sourced from the "get_league" function of the Extract module
    
    :returns: List of dictionaries to be passed onto the SQL insert/update statements.
    '''

    keys = [] 
    values = []
    league_dict = transform_first_layer(json_data)
    league_dict['year'] = year
    
    return dict_to_list(league_dict)

# roster
def transform_rosters(json_data, year):
    '''
    Data is sourced from the "rosters.json" file which is sourced from the "get_rosters" function of the Extract module

    :param filename: 
    :return: List of dictionaries to be passed onto the SQL insert/update statements.
    '''


    table_entries = []
    for roster_dict in json_data:
        entry_dict = transform_first_layer(roster_dict)
        entry_dict['wins'] = roster_dict['settings']['wins']
        entry_dict['losses'] = roster_dict['settings']['losses']
        entry_dict['ties'] = roster_dict['settings']['ties']
        entry_dict['year'] = year
        table_entries.append(entry_dict)

    return dict_to_list(table_entries)

# rostered_player
def transform_roster_players(json_data, year):
    '''
    Data is sourced from the "rosters.json" file which is sourced from the "get_rosters" function of the Extract module
    
    :param filename: 
    :return: List of dictionaries to be passed onto the SQL insert/update statements.
    '''



    # get the player_id from all rostered players
    player_entry_list = []
    for roster in json_data:
        # Append the player_id for starter players
        for player_id in roster['starters']:
            starter = 1
            player_dict = {'player_id': player_id, 
                           'roster_id': roster['roster_id'], 
                           'owner_id': roster['owner_id'], 
                           'starter': starter,
                           'year': year,}
            player_entry_list.append(player_dict)

        # Append the player_id for bench players
        bench_players = set_uniqueness(roster['players'], roster['starters'])
        for player_id in bench_players:
            starter = 0
            player_dict = {'player_id': player_id, 
                           'roster_id': roster['roster_id'], 
                           'owner_id': roster['owner_id'], 
                           'starter': starter,
                           'year': year,}
            player_entry_list.append(player_dict)

    return dict_to_list(player_entry_list)

# user
def transform_users(json_data, year):
    '''
    Data is sourced from the "user_info.json" file which is sourced from the "get_user_info" function of the Extract module
    
    :param filename: 
    :return: List of dictionaries to be passed onto the SQL insert/update statements.
    '''


    table_entries = []
    for entry_dict in json_data:
        entry_dict = transform_first_layer(entry_dict)
        entry_dict['year'] = year
        table_entries.append(entry_dict)

    return dict_to_list(table_entries)


# roster_week
def transform_roster_weeks(json_data, week, year):
    '''
    Data is sourced from the "matchups_week_x.json" file which is sourced from the "get_matchups" function of the Extract module
    
    TODO: make the roster_weeks transformation stateless
    
    :param filename: 
    :return: List of dictionaries to be passed onto the SQL insert/update statements.
    '''


    # get the roster_id and week_id for each entry
    roster_week_entry_list = []
    for roster in json_data:
        roster_dict = {'roster_id': roster['roster_id'],
                       'week_id': week,
                       'matchup_id': roster['matchup_id'],
                       'points': roster['points'],
                       'year': year,}
        roster_week_entry_list.append(roster_dict)

    return dict_to_list(roster_week_entry_list)


# player_week
def transform_player_week(json_data, week, year):
    '''
    Data is sourced from the "matchups_week_x.json" file which is sourced from the "get_matchups" function of the Extract module
    
    TODO: make the player_week transformation stateless
    
    :param filename: 
    :return: List of dictionaries to be passed onto the SQL insert/update statements.
    '''


    # get the player_id from all players that have a roster_id (players on rosters) during for the given matchup_id
    player_week_entry_list = []
    for roster in json_data:
        # Append the player_id for starter players
        for player_id in roster['starters']:
            starter = 1
            player_dict = {'player_id': player_id,
                           'roster_id': roster['roster_id'],
                           'week_id': week,
                           'matchup_id': roster['matchup_id'],
                           'starter': starter,
                           'year': year}
            player_week_entry_list.append(player_dict)

        # Append the player_id for bench players
        bench_players = set_uniqueness(roster['players'], roster['starters'])
        for player_id in bench_players:
            starter = 0
            player_dict = {'player_id': player_id,
                           'roster_id': roster['roster_id'],
                           'week_id': week,
                           'matchup_id': roster['matchup_id'],
                           'starter': starter,
                           'year': year}
            player_week_entry_list.append(player_dict)

    return dict_to_list(player_week_entry_list)


# league_transaction
def transform_transactions(json_data, year):
    '''
    Data is sourced from the "transactions.json" file which is sourced from the "get_transactions" function of the Extract module.
    
    TODO: get fields from json file that include the players involved in the transaction.
    
    :param filename: 
    :return: List of dictionaries to be passed onto the SQL insert/update statements.
    '''


    table_entries = []
    for entry_dict in json_data:
        entry_dict = transform_first_layer(entry_dict)
        entry_dict['year'] = year
        table_entries.append(entry_dict)

    return dict_to_list(table_entries)

# nfl_state
def transform_nfl_state(json_data, year):
    '''
    Data is sourced from the "nfl_state.json" file which is sourced from the "get_nfl_state" function of the Extract module.
    
    :param filename: 
    :return: List of dictionaries to be passed onto the SQL insert/update statements.
    '''


    entry_dict = transform_first_layer(json_data)
    entry_dict['year'] = year

    return dict_to_list(entry_dict)


# draft_info
def transform_draft_info(json_data, year):
    '''
    Data is sourced from the "draft_info.json" file which is sourced from the "get_draft_info" function of the Extract module
    
    :param filename: 
    :return: List of dictionaries to be passed onto the SQL insert/update statements.
    '''


    entry_dict = transform_first_layer(json_data)
    entry_dict['year'] = year

    return dict_to_list(entry_dict)


# draft_order
def transform_draft_order(json_data, year):
    '''
    Data is sourced from the "draft_order.json" file which is sourced from the "get_draft_order" function of the Extract module
    
    :param filename: 
    :return: List of dictionaries to be passed onto the SQL insert/update statements.
    '''


    draft_order_entries = []
    for user_id, draft_slot in json_data["draft_order"].items():
        draft_order_entry = {'user_id': user_id,
                             'draft_slot': draft_slot,
                             'draft_id': json_data['draft_id'],
                             'year': year}
        draft_order_entries.append(draft_order_entry)

    return dict_to_list(draft_order_entries)


# draft_pick
def transform_draft_picks(json_data, year):
    '''
    Data is sourced from the "draft_picks.json" file which is sourced from the "get_draft_picks" function of the Extract module
    
    :param filename: 
    :return: List of dictionaries to be passed onto the SQL insert/update statements.
    '''

    table_entries = []
    for entry_dict in json_data:
        entry_dict = transform_first_layer(entry_dict)
        entry_dict['year'] = year
        table_entries.append(entry_dict)

    return dict_to_list(table_entries)


# player
def transform_player_data(json_data, year):
    '''
    Data is sourced from the "player_data.json" file which is sourced from the "get_player_data" function of the Extract module
    
    :param filename: 
    :return: List of dictionaries to be passed onto the SQL insert/update statements.
    '''

    table_entries = []
    for player_id, player_dict in json_data.items():
        player_dict['year'] = year
        table_entries.append(player_dict)
    return dict_to_list(table_entries)

# General transform function
def transform_many(table_names, json_dict, year):
    '''
    This function runs transformations for the specified table names.
    
    Each table must have a transformation function programmed and included in this module.

    :param json_data:
    :param table_names:
    :return table_entries_list: Each element is a list of rows. Each row can be entered into a database table.

    TODO: Improve functionality by eliminating the need for a table_to_endpoint_map.
    '''

    table_to_endpoint_map = {'league': {'endpoint': 'league', 'transform_func': transform_league, 'weekly': False},
                             'roster': {'endpoint': 'rosters', 'transform_func': transform_rosters, 'weekly': False},
                             'rostered_player': {'endpoint': 'rosters', 'transform_func': transform_roster_players, 'weekly': False},
                             'user': {'endpoint': 'user_info', 'transform_func': transform_users, 'weekly': False},
                             'roster_week': {'endpoint': 'matchups_week_', 'transform_func': transform_roster_weeks, 'weekly': True},
                             'player_week': {'endpoint': 'matchups_week_', 'transform_func': transform_player_week, 'weekly': True},
                             'league_transaction': {'endpoint': 'transactions', 'transform_func': transform_transactions, 'weekly': False},
                             'nfl_state': {'endpoint': 'nfl_state', 'transform_func': transform_nfl_state, 'weekly': False},
                             'draft_info': {'endpoint': 'draft_info', 'transform_func': transform_draft_info, 'weekly': False},
                             'draft_order': {'endpoint': 'draft_info', 'transform_func': transform_draft_order, 'weekly': False},
                             'draft_pick': {'endpoint': 'draft_picks', 'transform_func': transform_draft_picks, 'weekly': False},
                             'player': {'endpoint': 'player_data', 'transform_func': transform_player_data, 'weekly': False},
                             }

    table_entries_dict = {}
    for table_name in table_names:
        try:
            table_to_endpoint_map[table_name]
        except ValueError as error:
            print(f'Table {table_name} does not have a transformation: ' + repr(error))

        transform_func = table_to_endpoint_map[table_name]['transform_func']

        if table_to_endpoint_map[table_name]['weekly']:
            table_entries = []
            for i in range(1, 19):
                endpoint = table_to_endpoint_map[table_name]['endpoint'] + '{:02d}'.format(i)
                json_data = json_dict[endpoint]
                table_entries += transform_func(json_data, week=int(i), year=year)
        else:
            endpoint = table_to_endpoint_map[table_name]['endpoint']
            json_data = json_dict[endpoint]
            table_entries = transform_func(json_data, year)

        table_entries_dict[table_name] = table_entries
        
    return table_entries_dict

