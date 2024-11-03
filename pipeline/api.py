import pipeline.fantasy_db as fantasy_db
import pipeline.transform as transform
import pipeline.extract as extract
import json, os


class FantasyApi():
    def __init__(self):
        self.db_conn = None
        self.root_path = os.path.join(os.getcwd(), 'app')
        self.config_file_name = 'config.txt'
        self.config_file_name_path = os.path.join(self.root_path, self.config_file_name)
        self.download_file_path = None

        # Check if config file exists TODO: Move to api
        config_exists = False
        for file in os.listdir(self.root_path):
            if file == self.config_file_name:
                config_exists = True
        if config_exists == False:
            with open(self.config_file_name_path, 'w') as config_file:
                print('Successfully created config file.')

        # Check if config file is empty or corrupted
        config_empty = True
        config_corrupted = False
        with open(self.config_file_name_path, 'r') as config_file:
            pass

    def create(self):
        # Create a record in a table
        pass

    def read(self, table_name):
        # Retrieve [information requested from ORM]
        if fantasy_db.table_exists(self.db_conn, table_name):
            table_data = fantasy_db.get_table_data(self.db_conn, table_name)
            return table_data
        else:
            # table_data = None
            raise ValueError(f'Table {table_name} not found.')

    def update(self):
        # Update a table record
        pass

    def delete(self):
        # Delete a table record
        pass

    def login(self, auth):
        # Connect to user's database
        # TODO: Move login file to database
        username = auth['username']

        with open(self.config_file_name_path, 'r') as config_file:
            for row in config_file:
                account_info = json.loads(row)
                if username == account_info['username']:
                    valid_login = True
                    database = account_info['database']
                    league_id = account_info[
                        'league_id']  # Replace with call to database for league_id associated with username
                    # draft_ids = account_info[
                    #     'draft_ids']  # Replace with call to database for draft_ids assoc. with username



        if valid_login:
            self.db_conn = fantasy_db.create_connection(database)
            if league_id == '':
                response = {'valid_login': True, 'username': username, 'league_id': league_id}
            else:
                response = {'valid_login': True, 'username': username, 'league_id': league_id}
        else:
            response = {'valid_login': False}

        return response

    def load_sleeper_tables(self, sleeper_api_params, year, include_player_data=True):
        '''Run the sleeper ETL and override existing data sourced from sleeper.
        :return:
        '''

        table_names = ['league', 'roster', 'rostered_player', 'user', 'roster_week', 'player_week',
                       'nfl_state', 'draft_info', 'draft_pick', 'draft_order', 'player']
        # 'league_transaction'

        json_dict = extract.extract_all(api_params=sleeper_api_params, include_player_data=include_player_data)

        table_entries_dict = transform.transform_many(table_names, json_dict, year)

        # self = load.FantasyApi(db_conn)
        self.update_tables(table_entries_dict, _overwrite=True)

    def update_tables(self, table_entries_dict, _overwrite=False):
        '''
        This function takes a list of dictionaries and inserts each dictionary into the table as a row.
        
        :param conn: connection to database
        :param table_names:
        :param _overwrite: Bool. Optional. If set to True, the table will be recreated and re-populated with table_entries
        :return:
        '''

        # Extract
        for table_name in table_entries_dict:
            table_entries = table_entries_dict[table_name]
            if fantasy_db.table_exists(self.db_conn, table_name):
                if _overwrite:
                    table_fields = transform.get_json_keys(table_entries)
                    fantasy_db.delete_table(self.db_conn, table_name)  # TODO: Code method within class???
                    fantasy_db.create_table(self.db_conn, table_name, table_fields)  # TODO: Code method within class???
                    self.db_conn.commit()
                else:
                    table_fields = [i for i in fantasy_db.get_table_column_names(self.db_conn, table_name)[1:]]
            else:
                table_fields = transform.get_json_keys(table_entries)
                fantasy_db.create_table(self.db_conn, table_name, table_fields)
                self.db_conn.commit()

            # Insert table_entries into table
            table_entries = transform.dict_to_list(table_entries)
            for entry_dict in table_entries:
                row_vals = transform.convert_dict_to_table_row(entry_dict, table_fields)
                fantasy_db.create_table_row(self.db_conn, table_name, row_vals)

            self.db_conn.commit()

    def get_tables(self, table_names=[]):
        '''
        This function is incomplete and may be deprecated in the future. 
        
        :param table_names:
        :return:
        '''
        table_data_list = []

        # Append table_data
        for table_name in table_names:
            if fantasy_db.table_exists(self.db_conn, table_name):
                table_data = fantasy_db.get_table_data(self.db_conn, table_name)
            else:
                table_data = None
                print(f'Table {table_name} not found.')
            table_data_list.append(table_data)

        return table_data_list

    def create_account(self, auth):
        '''Trigger the instantiation of a new database and create an account entry in the configuration file.'''

        username = auth['username']
        database = username + '.db'

        if self.validate_account_creation(username, database):
            fantasy_db.create_db(database)
            self.db_conn = fantasy_db.create_connection(database)
            full_database_path = os.path.join(self.root_path, database)

            # Instantiate Database
            self.instantiate_dynamic_tables()

            # Create config file entry
            with open(self.config_file_name_path, 'a') as config_file:
                entry_dict = {"username": username, "database": database,
                              "directory": self.root_path, 'league_id': ''}
                config_file.write(json.dumps(entry_dict) + '\n')
                print('written kitten', entry_dict)
        else:
            # TODO: Return an error
            pass

    def validate_account_creation(self, username, file_name):
        return True

    def instantiate_dynamic_tables(self):
        table_name = 'season'
        table_fields = ['year', 'league_id', 'draft_id']

        if not fantasy_db.table_exists(self.db_conn, table_name):
            fantasy_db.create_table(self.db_conn, table_name, table_fields)
            self.db_conn.commit()

    def _create_table(self, table_name, table_fields=None):
        # if table_entries != None:
        #     table_fields = transform.get_json_keys(table_entries)
        #     fantasy_db.create_table(self.db_conn, table_name, table_fields)
        #     self.update_tables(table_names=[table_name], _overwrite=False)
        # else:
        #     fantasy_db.create_table(self.db_conn, table_name, table_fields)
        # print(f'{table_name} table has been created.')
        pass

    def _delete_table(self, table_name):
        if fantasy_db.table_exists(self.db_conn, table_name):
            fantasy_db.delete_table(self.db_conn, table_name)


if __name__ == '__main__':
    pass

