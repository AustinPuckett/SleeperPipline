import os
import json
import sqlite3 as sql
import analysis.standard_queries as standard_queries
import pipeline.api as api


class AccountModel():
    '''TODO: Change working directory once this program is converted to a .exe'''

    def __init__(self):

        self.api = api.FantasyApi()
        self.username = None
        self.league_id = None
        # self.draft_ids = None

    def validate_login(self, username):
        # TODO: update login validation to require more than username
        # login_response = {'success': False, 'message': None}

        auth = {'username': username}
        login_response = self.api.login(auth)
        if login_response['valid_login'] == True:
            self.league_id = login_response['league_id']
            # self.draft_ids = login_response['draft_ids']
        else:
            # TODO: Raise Error?
            pass

        return login_response

    def get_user_count(self):
        # Retrieve the total number of users
        # TODO: Program
        return 1

    def create_account(self, username):
        '''Trigger the instantiation of a new database and create an account entry in the configuration file.'''

        auth = {'username': username}

        self.api.create_account(auth)

    def refresh_sleeper_data(self, include_player_data=True):
        # TODO: Eliminate refresh of draft_id unless specified
        self.api.load_sleeper_tables(sleeper_api_params={'league_id': self.league_id,
                                                        'draft_id': self.draft_ids[0],},
                                     year=2024,
                                     include_player_data=True)


class StartModel():
    def __init__(self, api):
        ...
        self.api = api
        self.visuals = ['Schedule Luck', 'Roster Week Points']
        self.eval_week = self.get_week()

    def get_luck_factor_df(self, week, year):
        df = standard_queries.get_luck_factor_df(self.api.db_conn, week, year)
        return df

    def get_roster_week_points_df(self, week, year):
        df = standard_queries.get_roster_week_points_df(self.api.db_conn, week, year)
        return df

    def get_week(self):
        return 4


class SeasonModel():
    def __init__(self, api):
        ...
        self.api = api
        self.league_id = None
        self.draft_id = None
        self.year = None

    def get(self, year):
        self.year = year

        season_info = standard_queries.get_season(self.api.db_conn, year)
        return season_info

    def create(self):
        standard_queries.create_season(self.api.db_conn, self.year, self.league_id, self.draft_id)

    def get_all(self):
        season_info = standard_queries.get_all_seasons(self.api.db_conn)
        return season_info

    def load_season_data(self):
        api_params = {'league_id': int(self.league_id), 'draft_id': int(self.draft_id), 'week': None}
        self.api.load_sleeper_tables(api_params, int(self.year), include_player_data=True)


