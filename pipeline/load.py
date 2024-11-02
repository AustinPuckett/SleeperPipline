import pipeline.fantasy_db as fantasy_db
import pipeline.transform as transform
import pipeline.extract as extract
import time

def update_all_tables(api, _overwrite=True):
    table_names = ['league', 'roster', 'rostered_player', 'user', 'nfl_state', 'draft_info', 'draft_order',
                   'draft_pick', 'league_transaction', 'roster_week', 'player_week', 'player']

    api.update_tables(table_names, _overwrite=_overwrite)


class FantasyApi():
    def __init__(self, auth):
        # Find database provided by auth
        # TODO: Replace dummys
        auth = {'key': 'True'} # Dummy auth
        db = 'sample.db' # Dummy db
        if auth['key'] == 'True': # if provided auth_key matches saved auth_key, permit access
            self.db_conn = fantasy_db.create_connection(db)

    def create(self):
        # Create a record in a table
        pass

    def read(self):
        # Retrieve [information requested from ORM]
        pass

    def update(self):
        # Update a table record
        pass

    def delete(self):
        # Delete a table record
        pass

    def load_sleeper_tables(self, sleeper_api_params, year, include_player_data=True):
        '''Run the sleeper ETL and override existing data sourced from sleeper.
        :return:
        '''

        table_names = ['league', 'roster', 'rostered_player', 'user', 'roster_week', 'player_week',
                       'league_transaction', 'nfl_state', 'draft_info', 'draft_pick', 'draft_order', 'player']

        json_dict = extract.extract_all(api_params=sleeper_api_params, include_player_data=include_player_data)

        table_entries_dict = transform.transform_many(table_names, json_dict, year)

        # self = load.FantasyApi(db_conn)
        self.update_tables(table_entries_dict, _overwrite=False)


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
            t0 = time.time()
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
            t1 = time.time()
            print(t1 - t0, f" seconds to create table {table_name}.")

            # Insert table_entries into table
            table_entries = transform.dict_to_list(table_entries)
            for entry_dict in table_entries:
                row_vals = transform.convert_dict_to_table_row(entry_dict, table_fields)
                fantasy_db.create_table_row(self.db_conn, table_name, row_vals)

            self.db_conn.commit()
            t2 = time.time()
            print(t2 - t1, f" seconds to create table entries for {table_name}.")

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
    # t0 = time.time()
    # num_weeks = 18
    #
    # db = 'fantasy.db'
    # api = FantasyApi(db)
    #
    # table_names = ['league', 'roster', 'rostered_player', 'user', 'nfl_state',
    #                'draft_info', 'draft_order', 'draft_pick', 'league_transaction']

    # table_names = ['roster_week', 'player_week']
    # table_names = ['player']

    # api.update_tables(table_names, _overwrite=True)
    #
    # t1 = time.time()
    # print(t1-t0, f" seconds to update tables {repr([i for i in table_names])}.")

    # i = 0
    # table_column_names = fantasy_db.get_table_column_names(api.conn, table_names[i])
    # table_data_list = api.get_tables(table_names)

    # [print(i) for i in table_data_list[i]]
    # print(table_column_names)

