
def create_views(engine):
    queries = [
        #top scorers 
        """
        CREATE OR REPLACE VIEW top_scorers AS
        SELECT player_name, team_name, season_id, goals, goal_assist_per_90
        FROM player_stats_gold
        ORDER BY goals DESC
        LIMIT 50;
        """,
        #top assists
        """
        CREATE OR REPLACE VIEW top_assisters AS
        SELECT player_name, team_name, season_id, assists, goal_assist_per_90
        FROM player_stats_gold
        ORDER BY assists DESC
        LIMIT 50;
        """,
        #disciplined
        """
        CREATE OR REPLACE VIEW disciplined_players AS
        SELECT player_name, team_name, season_id, yellow_per_appearance, red_per_appearance
        FROM player_stats_gold
        ORDER BY red_per_appearance DESC, yellow_per_appearance DESC
        LIMIT 50;
        """,
        #top pass accuracy 
        """
        CREATE OR REPLACE VIEW top_pass_accuracy AS
        SELECT player_name, team_name, season_id, passes_accuracy_percent
        FROM player_stats_gold
        ORDER BY passes_accuracy_percent DESC
        LIMIT 50;
        """,
        #aggregated stats by team -
        """
        DROP VIEW IF EXISTS team_aggregates;
        CREATE TABLE team_aggregates AS
        SELECT 
            team_name,
            season_id,
            SUM(goals) AS total_goals,
            SUM(assists) AS total_assists,
            SUM(appearances) AS total_appearances,
            AVG(passes_accuracy_percent) AS avg_pass_accuracy,
            SUM(yellow_cards) AS total_yellow,
            SUM(red_cards) AS total_red,
            SUM(tackles) AS total_tackles,
            SUM(interceptions) AS total_interceptions,
            SUM(fouls) AS total_fouls
        FROM player_stats_gold
        GROUP BY team_name, season_id
        ORDER BY total_goals DESC;
        """
    ]
    
    with engine.begin() as conn:
        for q in queries:
            conn.execute(q)
