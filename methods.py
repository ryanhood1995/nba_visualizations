from nba_api.stats.static import *
from nba_api.stats.endpoints import *
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
import time
pd.set_option('display.max_columns', None)


def get_teams_df():
    """ This methods returns a teams dataframe as well as a Python list containing all team ids. """
    nba_teams = teams.get_teams()
    df = pd.DataFrame.from_records(nba_teams)
    team_ids_list = df['id'].tolist()
    return df, team_ids_list


def get_players_df():
    """ This methods returns a players dataframe as well as a Python list containing all player ids. """
    nba_players = players.get_players()
    df = pd.DataFrame.from_records(nba_players)
    player_ids_list = df['id'].tolist()
    return df, player_ids_list


def get_player_awards(player_ids_list):
    """ This methods loops through a list of player ids, gathering their awards, and returning a resulting dataframe """
    df = pd.DataFrame(columns=['PERSON_ID', 'FIRST_NAME', 'LAST_NAME', 'TEAM', 'DESCRIPTION', 'ALL_NBA_TEAM_NUMBER',
                               'SEASON', 'MONTH', 'WEEK', 'CONFERENCE', 'TYPE', 'SUBTYPE1', 'SUBTYPE2', 'SUBTYPE3'])
    num_players = len(player_ids_list)
    print("Getting Player Awards")
    for player_id_idx in range(0, num_players):
        if player_id_idx % 100 == 0:
            print(f"Progress: {player_id_idx} out of {num_players}")
        player_id = player_ids_list[player_id_idx]
        try:
            player_awards = playerawards.PlayerAwards(player_id=player_id).get_data_frames()[0]
            df = pd.concat([df, player_awards])
        except:
            print(f"Exception Occurred on Player ID: {player_id}")
        time.sleep(1)
    return df


def get_games(team_ids_list):
    """ This methods loops through a list of team ids, gathering their associated games, and returning
    a resulting dataframe """
    df = pd.DataFrame(columns=['SEASON_ID', 'TEAM_ID', 'TEAM_ABBREVIATION', 'TEAM_NAME', 'GAME_ID', 'GAME_DATE',
                               'MATCHUP', 'WL', 'MIN', 'PTS', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT',
                               'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PF',
                               'PLUS_MINUS'])
    num_teams = len(team_ids_list)
    print("Getting Games")
    for team_id_idx in range(0, num_teams):
        print(f"Progress: {team_id_idx+1} out of {num_teams}")
        team_id = team_ids_list[team_id_idx]
        try:
            team_games = leaguegamefinder.LeagueGameFinder(team_id_nullable=team_id).get_data_frames()[0]
            df = pd.concat([df, team_games])
        except:
            print(f"Exception Occurred on Team ID: {team_id}")
        time.sleep(1)
    return df


def get_player_career_stats(player_ids_list):
    """ This methods loops through a list of player ids, gathering their career statistics, and returning
    a resulting dataframe """
    df = pd.DataFrame(columns=['PLAYER_ID', 'SEASON_ID', 'LEAGUE_ID', 'TEAM_ID', 'TEAM_ABBREVIATION', 'PLAYER_AGE',
                               'GP', 'GS', 'MIN', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT',
                               'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PF',
                               'PTS'])
    num_players = len(player_ids_list)
    print("Getting Career Stats")
    for player_id_idx in range(0, num_players):
        if player_id_idx % 100 == 0:
            print(f"Progress: {player_id_idx} out of {num_players}")
        player_id = player_ids_list[player_id_idx]
        try:
            player_career_stats = playercareerstats.PlayerCareerStats(player_id).get_data_frames()[0]
            df = pd.concat([df, player_career_stats])
        except:
            print(f"Exception Occurred on Player ID: {player_id}")
        time.sleep(1)
    return df


def get_player_pic_urls(player_ids_list):
    """ This method takes a list of player ids and returns a dataframe with urls that return stock images
     of each player. """
    url_list = []
    for player_id in player_ids_list:
        url = f"https://ak-static.cms.nba.com/wp-content/uploads/headshots/nba/latest/260x190/{player_id}.png"
        url_list.append([player_id, url])
        continue
    df = pd.DataFrame(data=url_list, columns=['player_id', 'url'])
    return df


def get_players_who_participated_in_season(season_id, season_players_list):
    """ This method takes a season id and a crosswalk list of seasons and players and finds the players who actually
     participated in that season. """
    players_who_participated_in_season = []
    for sublist in season_players_list:
        if sublist[0] == season_id:
            players_who_participated_in_season.append(sublist[1])
        continue
    return players_who_participated_in_season


def get_player_games(season_ids_list, season_type_list, season_players_list):
    """ This method takes a list of player ids, season ids, and season types, and returns stats from the games by those
     players in those seasons. """
    df = pd.DataFrame(columns=['SEASON_ID', 'Player_ID', 'Game_ID', 'GAME_DATE', 'MATCHUP', 'WL', 'MIN', 'FGM', 'FGA',
                               'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB',
                               'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'PLUS_MINUS', 'VIDEO_AVAILABLE', 'SEASON_TYPE'])
    print("Getting Player Games")
    for season_id in season_ids_list:
        # We need to find the player ids that actually participated in this season.
        players_who_participated_in_season = get_players_who_participated_in_season(season_id, season_players_list)
        num_players = len(players_who_participated_in_season)
        for player_id_idx in range(0, num_players):
            print(f"Progress: {player_id_idx} out of {num_players}")
            player_id = players_who_participated_in_season[player_id_idx]
            for season_type in season_type_list:
                try:
                    player_games = playergamelog.PlayerGameLog(player_id=player_id, season=season_id,
                                                                   season_type_all_star=season_type).get_data_frames()[0]
                    player_games['SEASON_TYPE'] = season_type
                    df = pd.concat([df, player_games])
                except:
                    print(f"Exception Occurred on Player ID: {player_id} Season ID: {season_id} and Season Type: {season_type}")
                continue
            continue
        continue
    time.sleep(1)
    return df


def get_team_metrics(season_ids_list, season_type_list):
    """ This method takes a list of season ids and season types and returns stats from all teams for those seasons. """
    df = pd.DataFrame(columns=['TEAM_NAME', 'TEAM_ID', 'GP', 'W', 'L', 'W_PCT', 'MIN', 'E_OFF_RATING', 'E_DEF_RATING',
                               'E_NET_RATING', 'E_PACE', 'E_AST_RATIO', 'E_OREB_PCT', 'E_DREB_PCT', 'E_REB_PCT',
                               'E_TM_TOV_PCT', 'GP_RANK', 'W_RANK', 'L_RANK', 'W_PCT_RANK', 'MIN_RANK',
                               'E_OFF_RATING_RANK', 'E_DEF_RATING_RANK', 'E_NET_RATING_RANK', 'E_AST_RATIO_RANK',
                               'E_OREB_PCT_RANK', 'E_DREB_PCT_RANK', 'E_REB_PCT_RANK', 'E_TM_TOV_PCT_RANK',
                               'E_PACE_RANK', 'SEASON_ID', 'SEASON_TYPE'])
    print("Getting Team Metrics")
    num_seasons = len(season_ids_list)
    count = 1
    for season_id in season_ids_list:
        print(f"Progress {count} out of {num_seasons}")
        count = count + 1
        for season_type in season_type_list:
            try:
                team_metrics = teamestimatedmetrics.TeamEstimatedMetrics(season=season_id, league_id='00', season_type=season_type).get_data_frames()[0]
                team_metrics['SEASON_ID'] = season_id
                team_metrics['SEASON_TYPE'] = season_type
                df = pd.concat([df, team_metrics])
            except:
                print(f"Exception Occured on Season ID: {season_id} and Season Type: {season_type}")
        time.sleep(1)
    return df


if __name__ == '__main__':
    teams_df, team_ids_list = get_teams_df()
    players_df, player_ids_list = get_players_df()



