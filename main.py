import db
import methods


if __name__ == '__main__':
    teams_df, team_ids_list = methods.get_teams_df()
    db.df_to_postgres(teams_df, 'teams')

    players_df, player_ids_list = methods.get_players_df()
    db.df_to_postgres(players_df, 'players')

    games_df = methods.get_games(team_ids_list)
    db.df_to_postgres(games_df, 'games')

    player_career_stats_df = methods.get_player_career_stats(player_ids_list)
    db.df_to_postgres(player_career_stats_df, 'player_career_stats')

    player_awards_df = methods.get_player_awards(player_ids_list)
    db.df_to_postgres(player_awards_df, 'player_awards')

    player_pic_urls_df = methods.get_player_pic_urls(player_ids_list)
    db.df_to_postgres(player_pic_urls_df, 'player_pic_urls')

    season_players_list = player_career_stats_df[['SEASON_ID', 'PLAYER_ID']].values.tolist()
    player_games_df = methods.get_player_games(['2022-23'], season_type_list=['Regular Season'], season_players_list=season_players_list)
    db.df_to_postgres(player_games_df, 'player_games')

    team_metrics_df = methods.get_team_metrics(['2022-23', '2021-22', '2020-21', '2019-20', '2018-19', '2017-18'], ['Regular Season', 'Playoffs'])
    db.df_to_postgres(team_metrics_df, 'team_metrics')




