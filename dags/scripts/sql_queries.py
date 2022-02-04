class FactSql:
    achievement_fact_insert = """
    --Create temp table for staged data
    CREATE TEMPORARY TABLE temp_fact (
        steam_id bigint,
        name text,
        game_name text,
        unlock_ts timestamp with time zone
    )ON COMMIT DROP;
    
    --Copy data from S3 Bucket to Database
    select aws_s3.table_import_from_s3 (
       'temp_fact',
       'steam_id, name, game_name, unlock_ts',
       '(FORMAT CSV, HEADER)',
       '{bucket}',
       '{bucket_key}',
       '{region}'
    );
    
    -- Insert data that does not exist in our dimension table
    INSERT INTO rust_data_warehouse."Achievement_Fact"(player_sk, achievement_sk, game_sk, date_sk, time)
    SELECT player_sk, achievement_sk, game_sk, date_sk, tf.unlock_ts::time WITH TIME ZONE as time  FROM temp_fact as tf
    INNER JOIN rust_data_warehouse."Achievements_Dim" as ad
        ON tf.name = ad.name
    INNER JOIN rust_data_warehouse."Player_Dim" as pd
        ON tf.steam_id = pd.steam_id
    INNER JOIN rust_data_warehouse."Game_Dim" as gd
        ON tf.game_name = gd.name
    INNER JOIN rust_data_warehouse."Date_Dim" as dd
        ON DATE(tf.unlock_ts) = dd.full_date;
    """

    badges_insert_sql = """
    --Create temp table for staged data
    CREATE TEMPORARY TABLE temp_fact (
        steam_id bigint, 
        badge_id bigint, 
        app_id bigint, 
        community_item_id bigint,  
        xp integer, 
        level integer, 
        completion_time timestamp with time zone, 
        scarcity integer,  
        steam_level integer
    )ON COMMIT DROP;
    
    --Copy data from S3 Bucket to Database
    select aws_s3.table_import_from_s3 (
       'temp_fact',
       'steam_id, badge_id, app_id, community_item_id, xp, level, completion_time, scarcity, steam_level',
       '(FORMAT CSV, HEADER)',
       'rust-cheaters',
       'data-lake/staged/steam/badges_fact/2022/02/04/2022-02-04T09_43_38Z_to_2022-02-04T10_43_38Z.csv',
       'us-east-1'
    );
        
    -- Insert data that does not exist in our dimension table
    INSERT INTO rust_data_warehouse."Badges_Fact"(player_sk, badge_sk, date_sk, scarcity, steam_level, time)
    SELECT player_sk, badge_sk, date_sk, scarcity, steam_level, tf.completion_time::time WITH TIME ZONE as time FROM temp_fact as tf
     INNER JOIN rust_data_warehouse."Badges_Dim" as bd
        ON tf.badge_id = bd.badge_id AND tf.app_id = bd.app_id AND tf.community_item_id = bd.community_item_id AND tf.xp = bd.xp AND tf.level = bd.level
     INNER JOIN rust_data_warehouse."Player_Dim" as pd
        ON tf.steam_id = pd.steam_id 
     INNER JOIN rust_data_warehouse."Date_Dim" as dd
        ON DATE(tf.completion_time) = dd.full_date;
    """