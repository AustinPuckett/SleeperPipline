import sqlite3 as sql
import pandas as pd
import pipeline.fantasy_db as fantasy_db
import pipeline.sleeper_etl as sleeper_etl

def get_roster(conn, roster_id, week, eval_year, starters_only=False):
    '''Warning: This function uses string formatting of the SQL statement in place of a bind'''

    cursor = conn.cursor()

    query = f'''SELECT r.roster_id, pw.week_id, pw.player_id, pw.starter, pw.matchup_id, p.full_name,
                p.fantasy_positions, p.injury_status, p.team,
                rproj."PROJ. FPTS" as "Ros Proj", wproj."PROJ. FPTS" as "Week Proj"
                FROM roster r
                LEFT JOIN player_week pw ON r.roster_id = pw.roster_id
                LEFT JOIN player p ON pw.player_id = p.player_id
                LEFT JOIN ros_projections rproj ON p.full_name = rproj."PLAYER NAME" AND p.team = rproj.TEAM
                LEFT JOIN week_projections wproj ON p.full_name = wproj."PLAYER NAME" AND p.team = rproj.TEAM AND
                 pw.week_id = wproj.week
                WHERE r.roster_id = {roster_id} AND pw.week_id = {week} AND r.year = {eval_year}
                '''
    roster = pd.read_sql(query, conn)
    # roster = roster.loc[roster['full_name'].isnull(), 'full_name'] = roster['team']
    # roster.loc[roster['full_name'].isnull(), 'full_name'] = roster[roster['full_name'].isnull()]['team']

    # conn.commit()
    cursor.close()

    return roster

def get_player(full_name, pos, team=None):
    pass

def get_roster_df(conn, eval_year):
    roster_df = pd.read_sql_query("SELECT * FROM roster WHERE year = ?", conn, params=(eval_year,))
    return roster_df

def get_league_df(conn, eval_year):
    league_df = pd.read_sql_query("SELECT * FROM league WHERE year = ?", conn, params=(eval_year,))
    return league_df

def get_user_df(conn, eval_year):
    user_df = pd.read_sql_query("SELECT * FROM user WHERE year = ?", conn, params=(eval_year,))
    return user_df

def get_roster_week_df(conn, eval_year):
    roster_week_df = pd.read_sql_query("SELECT * FROM roster_week WHERE year = ?", conn, params=(eval_year,))
    return roster_week_df

def get_all_time_roster_week_df(conn, start_year):
    roster_week_df = pd.read_sql_query("SELECT * FROM roster_week WHERE year >= ?", conn, params=(start_year,))
    return roster_week_df

def all_time_points_scored_by_roster_week(conn, start_year, ascending=False):
    final_week = 14
    current_week = 5
    df = pd.read_sql_query('''SELECT rw.points, rw.roster_id, rw.week_id, rw.year, rw.matchup_id, r.owner_id, u.user_id, 
                                          u.display_name FROM roster_week rw 
                                          LEFT JOIN roster r ON r.roster_id = rw.roster_id AND r.year = rw.year
                                          LEFT JOIN user u ON u.user_id = r.owner_id AND u.year = rw.year
                                          WHERE rw.year >= ?''', conn, params=(start_year,))

    filtered_df = df[~((df['year'] == 2023) & (df['week_id'] > current_week))]
    filtered_df = filtered_df[~(filtered_df['matchup_id'].isna())]
    filtered_df = filtered_df.sort_values(by='points', ascending=ascending)

    return filtered_df

def get_luck_factor_df(conn, eval_week, eval_year):
    roster_week_df = get_roster_week_df(conn, eval_year)
    roster_df = get_roster_df(conn, eval_year)
    user_df = get_user_df(conn, eval_year)

    # Points by Roster week
    df_1 = (
            roster_week_df
            .drop_duplicates(subset=['roster_id', 'week_id', 'matchup_id'])
            .merge(roster_df, how='left', left_on='roster_id', right_on='roster_id')
            .merge(user_df, how='left', left_on='owner_id', right_on='user_id')
            .loc[lambda df: df.week_id <= eval_week]
            .loc[:, ['display_name', 'roster_id', 'week_id', 'wins', 'losses', 'points']]
            .sort_values(by='week_id', ascending=False)
            )

    df_1['weekly_points_rank'] = (
                                    df_1
                                    .groupby('week_id')
                                    ['points']
                                    .rank(method='min', ascending=False)
                                  )

    df_2 = (
            df_1
            .groupby(['roster_id', 'display_name'])).sum()

    df_2['true_games'] = eval_week * 11
    df_2['true_losses'] = df_2['weekly_points_rank'] - eval_week
    df_2['true_wins'] = df_2['true_games'] - df_2['true_losses']
    df_2['true_win_percentage'] = df_2['true_wins'] / df_2['true_games']
    df_2 = df_2.sort_values('true_win_percentage', ascending=False)
    # df_2.to_html(r'')
    df_2['true_wins'] = df_2['true_wins'] / 11
    df_2['true_losses'] = df_2['true_losses'] / 11
    df_2['wins'] = df_2['wins'] / eval_week
    df_2['losses'] = df_2['losses'] / eval_week
    df_2['luck_factor'] = df_2['wins'] - df_2['true_wins']

    luck_factor_df = df_2.sort_values('luck_factor', ascending=True)

    return luck_factor_df.reset_index()

def get_roster_week_points_df(conn, eval_week, eval_year):
    roster_week_df = get_roster_week_df(conn, eval_year)
    roster_df = get_roster_df(conn, eval_year)
    user_df = get_user_df(conn, eval_year)

    # Points by Roster week
    df_1 = (
            roster_week_df
            .drop_duplicates(subset=['roster_id', 'week_id', 'matchup_id'])
            .merge(roster_df, how='left', left_on='roster_id', right_on='roster_id')
            .merge(user_df, how='left', left_on='owner_id', right_on='user_id')
            .loc[lambda df: df.week_id <= eval_week]
            .loc[:, ['display_name', 'roster_id', 'week_id', 'wins', 'losses', 'points']]
            .sort_values(by='week_id', ascending=False)
            )

    df_1['weekly_points_rank'] = (
                                    df_1
                                    .groupby('week_id')
                                    ['points']
                                    .rank(method='min', ascending=False)
                                  )

    roster_week_points_df = df_1.sort_values('points', ascending=False)

    return roster_week_points_df.reset_index()

def get_season(conn, year):
    season_df = pd.read_sql_query("SELECT * FROM season", conn)
    season_df = season_df[season_df['year'] == year]
    return season_df

def get_all_seasons(conn):
    season_df = pd.read_sql_query("SELECT * FROM season", conn)
    return season_df

def create_season(conn, year, league_id, draft_id):
    table_name = 'season'
    row_vals = [year, league_id, draft_id]
    fantasy_db.create_table_row(conn, table_name, row_vals)
    conn.commit()

def create_season_table(conn):
    table_name = 'season'
    table_fields = ['year', 'league_id', 'draft_id']

    if not fantasy_db.table_exists(conn, table_name):
        fantasy_db.create_table(conn, table_name, table_fields)
        conn.commit()

def create_db(db_file_name):
    conn = sql.connect(db_file_name)
    conn.close()

def connect_to_db(db_file_name):
    return sql.connect(db_file_name)

def load_season_data(conn, api_params, year):
    sleeper_etl.run_sleeper_etl(conn, api_params, year)


if __name__ == '__main__':
    db = 'fantasy.db'
    conn = sql.connect(db)

    roster = get_roster(conn, roster_id=7, week=2, starters_only=False)
    # print(roster.head(5))
    # print(roster.iloc[0])
    conn.close()
