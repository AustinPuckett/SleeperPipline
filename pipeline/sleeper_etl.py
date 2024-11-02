import pipeline.extract as extract
import pipeline.transform as transform
import pipeline.api as load
import time


def run_sleeper_etl(db_conn, api_params, year, include_player_data=True):
    # TODO: remove hard coded api params

    table_names = ['league', 'roster', 'rostered_player', 'user', 'roster_week', 'player_week', 'league_transaction',
                   'nfl_state', 'draft_info', 'draft_pick', 'draft_order', 'player']

    # api_params = {'league_id': '983048181460119552', 'draft_id': '983048181460119553', 'week': week}

    t0 = time.time()
    json_dict = extract.extract_all(api_params=api_params, include_player_data=include_player_data)
    t1 = time.time()
    print(t1 - t0, " seconds to extract data from the Sleeper API.")

    t0 = time.time()
    table_entries_dict = transform.transform_many(table_names, json_dict, year)
    t1 = time.time()
    print(t1 - t0, " seconds to transform Sleeper API data.")

    t0 = time.time()
    api = load.FantasyApi(db_conn)
    api.update_tables(table_entries_dict, _overwrite=False)
    t1 = time.time()
    print(t1 - t0, " seconds to extract load data.")
