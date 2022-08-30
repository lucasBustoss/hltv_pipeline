from gc import get_stats
from os.path import join
import argparse
import datetime 

from pyspark.sql.functions import col, array, explode, lit
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

def save_df(df, table):
    df.write.format("jdbc")\
        .options(
            url="jdbc:postgresql://hltv_data:5432/hltv",
            dbtable=f'raw.{table}',
            user="admin",
            password="123",
            driver="org.postgresql.Driver"
            )\
        .mode('append')\
        .save()
  
def get_match_df(raw_df, execution_date):
    match_df = raw_df\
        .withColumn('date', lit(execution_date))\
        .selectExpr(
            'match_id', 
            'type',
            'date',
            'best_of_type',
            'teams.team_id[0] as team1_id',
            'teams.team_id[1] as team2_id',
            'teams.score[0] as team1_score',
            'teams.score[1] as team2_score'
        )\

    save_df(match_df.dropDuplicates(), 'matchs')

def get_teams_df(raw_df):
    teams_df = raw_df\
        .select(explode('teams').alias('teams'))\
        .select(
            'teams.team_id', 
            'teams.name',
            'teams.link'
        )

    save_df(teams_df.dropDuplicates(), 'teams')

def get_players_df(raw_df):
    df_players = raw_df\
        .select('*')\
        .withColumn('stats', explode('stats'))\
        .withColumn('players', explode(array('stats.team1.total', 'stats.team2.total')))\
        .withColumn('players', explode('players'))\
        .select(col('match_id'), 'players.*')\
        .select('player_id', 'team_id', 'nick')

    save_df(df_players.dropDuplicates(), 'players')

def get_maps_df(raw_df):
    df_maps = raw_df\
        .select('*', 'maps.*')\
        .withColumn('ban', explode('bans'))\
        .withColumn('pick', explode('picks'))\
        .select('match_id', 'ban', 'pick')

    df_bans = get_maps_details_df(df_maps, 'ban')
    df_picks = get_maps_details_df(df_maps, 'pick')

    df_map_results = get_map_results_df(df_picks)
    
    df_picks_without_result = df_picks\
        .select(
            'match_id',
            'type',
            'choice_number',
            'map_name', 
            'team_id'
        )

    save_df(df_bans.dropDuplicates(), 'maps_vetos')
    save_df(df_picks_without_result.dropDuplicates(), 'maps_vetos')
    save_df(df_map_results.dropDuplicates(), 'maps_results')

def get_maps_details_df(df_maps, type):
    df_maps_details = df_maps\
        .select(
            'match_id',
            lit(f'{type}').alias('type'),
            f'{type}.*'    
        )\

    return df_maps_details

def get_map_results_df(df_picks):
    df_map_results = df_picks\
        .withColumn('results', explode('results'))\
        .select(
            'match_id',
            'choice_number',
            col('map_name'), 
            col('team_id').alias('pick_team_id'),
            'results.*'
        )

    return df_map_results

def get_stats_df(raw_df):
    df_stats = raw_df\
        .withColumn('stats', explode('stats'))\
        .select('match_id', 'stats.*')
        
    df_team1 = get_stats_team(df_stats, 'team1')
    df_team2 = get_stats_team(df_stats, 'team2')

    df_team1_ct = get_stats_side_df(df_team1, 'ct')
    df_team1_tr = get_stats_side_df(df_team1, 'tr')
    df_team1_total = get_stats_side_df(df_team1, 'total')

    df_team2_ct = get_stats_side_df(df_team2, 'ct')
    df_team2_tr = get_stats_side_df(df_team2, 'tr')
    df_team2_total = get_stats_side_df(df_team2, 'total')

    save_df(df_team1_ct.dropDuplicates(), 'match_stats')
    save_df(df_team1_tr.dropDuplicates(), 'match_stats')
    save_df(df_team1_total.dropDuplicates(), 'match_stats')

    save_df(df_team2_ct.dropDuplicates(), 'match_stats')
    save_df(df_team2_tr.dropDuplicates(), 'match_stats')
    save_df(df_team2_total.dropDuplicates(), 'match_stats')

def get_stats_team(df_stats, team):
    df_stats_team = df_stats\
        .select(
            'match_id',
            col('map').alias('map_name'),
            f'{team}.*'
        )

    return df_stats_team

def get_stats_side_df(df_stats_team, side):
    df_stats_side = df_stats_team\
        .withColumn(f'player_stats_{side}', explode(f'{side}'))\
        .select(
            'match_id', 
            lit(side).alias('side'), 
            'map_name', 
            f'player_stats_{side}.*'
        )

    return df_stats_side

def hltv_transform(spark, src, process_date):
    hltv_raw_df = spark.read.json(src)

    if(hltv_raw_df.count() != 0):
        match_date = (datetime.datetime.fromisoformat(process_date) - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        get_match_df(hltv_raw_df, match_date)
        get_teams_df(hltv_raw_df)
        get_players_df(hltv_raw_df)
        get_maps_df(hltv_raw_df)
        get_stats_df(hltv_raw_df)
    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(
        description='Spark HLTV Transformation'
    )
    parser.add_argument('--src', required=True)
    parser.add_argument('--process-date', required=True)
    args = parser.parse_args()

    spark = SparkSession\
        .builder\
        .appName('hltv_transformation')\
        .getOrCreate()
    
    hltv_transform(spark, args.src, args.process_date)
    
    """
    spark = SparkSession\
        .builder\
        .appName('hltv_transformation')\
        .getOrCreate()
    
    hltv_transform(
        spark, 
        '/raw/hltv/extract_date=2022-01-02/HLTV_20220102.json',
        '2022-01-02'
        )
    """