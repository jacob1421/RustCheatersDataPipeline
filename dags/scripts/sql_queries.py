class sql_queries:
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
    INSERT INTO {schema}."Achievement_Fact"(player_sk, achievement_sk, game_sk, date_sk, time)
    SELECT player_sk, achievement_sk, game_sk, date_sk, tf.unlock_ts::time WITH TIME ZONE as time  FROM temp_fact as tf
    INNER JOIN {schema}."Achievements_Dim" as ad
        ON tf.name = ad.name
    INNER JOIN {schema}."Player_Dim" as pd
        ON tf.steam_id = pd.steam_id
    INNER JOIN {schema}."Game_Dim" as gd
        ON tf.game_name = gd.name
    INNER JOIN {schema}."Date_Dim" as dd
        ON DATE(tf.unlock_ts) = dd.full_date;
    """

    badges_fact_insert = """
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
       '{bucket}',
       'data-lake/staged/steam/badges_fact/2022/02/04/2022-02-04T09_43_38Z_to_2022-02-04T10_43_38Z.csv',
       '{region}'
    );
        
    -- Insert data that does not exist in our dimension table
    INSERT INTO {schema}."Badges_Fact"(player_sk, badge_sk, date_sk, scarcity, steam_level, time)
    SELECT player_sk, badge_sk, date_sk, scarcity, steam_level, tf.completion_time::time WITH TIME ZONE as time FROM temp_fact as tf
     INNER JOIN {schema}."Badges_Dim" as bd
        ON tf.badge_id = bd.badge_id AND tf.app_id = bd.app_id AND tf.community_item_id = bd.community_item_id AND tf.xp = bd.xp AND tf.level = bd.level
     INNER JOIN {schema}."Player_Dim" as pd
        ON tf.steam_id = pd.steam_id 
     INNER JOIN {schema}."Date_Dim" as dd
        ON DATE(tf.completion_time) = dd.full_date;
    """

    bans_fact_insert = """
    --Create temp table for staged data
    CREATE TEMPORARY TABLE temp_fact (
        steam_id bigint, 
        last_ban_date timestamp with time zone, 
        num_vac_bans bigint, 
        num_game_bans bigint, 
        community_banned boolean,
        economy_ban text,
        vac_banned boolean
    )ON COMMIT DROP;
    
    --Copy data from S3 Bucket to Database
    select aws_s3.table_import_from_s3 (
       'temp_fact',
       'steam_id, last_ban_date, num_vac_bans, num_game_bans, community_banned, economy_ban, vac_banned',
       '(FORMAT CSV, HEADER)',
       '{bucket}',
       '{bucket_key}',
       '{region}'
    );
    
    INSERT INTO {schema}."Bans_Fact"(player_sk, date_sk, num_vac_bans, num_game_bans, community_banned, economy_ban, vac_banned)
    SELECT player_sk, date_sk, num_vac_bans, num_game_bans, community_banned, economy_ban, vac_banned FROM temp_fact as tf
      INNER JOIN {schema}."Player_Dim" as pd
        ON tf.steam_id = pd.steam_id 
      INNER JOIN {schema}."Date_Dim" as dd
        ON DATE(tf.last_ban_date) = dd.full_date;
    """

    friends_fact_insert = """
    --Create temp table for staged data
    CREATE TEMPORARY TABLE temp_fact (
        steam_id bigint,
        friend_steam_id bigint,
        friend_since timestamp with time zone, 
        relationship text
    )ON COMMIT DROP;
    
    --Copy data from S3 Bucket to Database
    select aws_s3.table_import_from_s3 (
       'temp_fact',
       'steam_id, friend_steam_id, friend_since, relationship',
       '(FORMAT CSV, HEADER)',
       '{bucket}',
       '{bucket_key}',
       '{region}'
    );
    
    INSERT INTO {schema}."Friends_Fact"(player_sk, player_friend_sk, date_sk, relationship_sk, time)
    SELECT player_sk, friend_sk, date_sk, relationship_sk, tf.friend_since::time WITH TIME ZONE as time FROM temp_fact as tf
      INNER JOIN {schema}."Player_Dim" as pd
        ON tf.steam_id = pd.steam_id 
      INNER JOIN {schema}."Friend_Dim" as fd
        ON tf.friend_steam_id = fd.steam_id 
      INNER JOIN {schema}."Relationship_Dim" as rr
        ON tf.relationship = rr.relationship
      INNER JOIN {schema}."Date_Dim" as dd
        ON DATE(tf.friend_since) = dd.full_date;
    """

    game_playing_banned_fact_insert = """
    --Create temp table for staged data
    CREATE TEMPORARY TABLE temp_fact (
        steam_id bigint,
        game_id bigint,
        date timestamp with time zone
    )ON COMMIT DROP;
    
    --Copy data from S3 Bucket to Database
    select aws_s3.table_import_from_s3 (
       'temp_fact',
       'steam_id, game_id, date',
       '(FORMAT CSV, HEADER)',
       '{bucket}',
       '{bucket_key}',
       '{region}'
    );
    
    INSERT INTO {schema}."Game_Playing_Banned_Fact"(player_sk, game_sk, date_sk)
    SELECT player_sk, game_sk, date_sk FROM temp_fact as tf
      INNER JOIN {schema}."Player_Dim" as pd
        ON tf.steam_id = pd.steam_id 
      INNER JOIN {schema}."Game_Dim" as gd
        ON tf.game_id = gd.game_id 
      INNER JOIN {schema}."Date_Dim" as dd
        ON DATE(tf.date) = dd.full_date;
    """

    game_playtime_fact_insert = """
    --Create temp table for staged data
    CREATE TEMPORARY TABLE temp_fact (
        steam_id bigint,
        game_id bigint,
        date timestamp with time zone, 
        playtime_windows_mins bigint, 
        playtime_mac_mins bigint, 
        playtime_linux_mins bigint, 
        playtime_two_weeks_mins bigint
    )ON COMMIT DROP;
    
    --Copy data from S3 Bucket to Database
    select aws_s3.table_import_from_s3 (
       'temp_fact',
       'steam_id, game_id, date, playtime_windows_mins, playtime_mac_mins, playtime_linux_mins, playtime_two_weeks_mins',
       '(FORMAT CSV, HEADER)',
       '{bucket}',
       '{bucket_key}',
       '{region}'
    );
    
    INSERT INTO {schema}."Game_Playtime_Fact"(player_sk, game_sk, date_sk, playtime_windows_mins, playtime_mac_mins, playtime_linux_mins, playtime_two_weeks)
    SELECT player_sk, game_sk, date_sk, playtime_windows_mins, playtime_mac_mins, playtime_linux_mins, playtime_two_weeks_mins FROM temp_fact as tf
      INNER JOIN {schema}."Player_Dim" as pd
        ON tf.steam_id = pd.steam_id 
      INNER JOIN {schema}."Game_Dim" as gd
        ON tf.game_id = gd.game_id 
      INNER JOIN {schema}."Date_Dim" as dd
        ON DATE(tf.date) = dd.full_date;
    """

    group_fact_insert = """
    --Create temp table for staged data
    CREATE TEMPORARY TABLE temp_fact (
        steam_id bigint,
        group_id bigint,
        date timestamp with time zone
    )ON COMMIT DROP;
    
    --Copy data from S3 Bucket to Database
    select aws_s3.table_import_from_s3 (
       'temp_fact',
       'steam_id, group_id, date',
       '(FORMAT CSV, HEADER)',
       '{bucket}',
       '{bucket_key}',
       '{region}'
    );
    
    INSERT INTO {schema}."Groups_Fact"(player_sk, group_sk, date_sk)
    SELECT player_sk, group_sk, date_sk FROM temp_fact as tf
      INNER JOIN {schema}."Player_Dim" as pd
        ON tf.steam_id = pd.steam_id 
      INNER JOIN {schema}."Group_Dim" as gd
        ON tf.group_id = gd.group_id
      INNER JOIN {schema}."Date_Dim" as dd
        ON DATE(tf.date) = dd.full_date;
    """

    stats_fact_insert = """
    --Create temp table for staged data
    CREATE TEMPORARY TABLE temp_fact (
        name text, 
        steam_id bigint, 
        game text, 
        date timestamp with time zone, 
        value real
    )ON COMMIT DROP;
    
    --Copy data from S3 Bucket to Database
    select aws_s3.table_import_from_s3 (
       'temp_fact',
       'name, steam_id, game, date, value',
       '(FORMAT CSV, HEADER)',
       '{bucket}',
       '{bucket_key}',
       '{region}'
    );
    
    INSERT INTO {schema}."Stats_Fact"(stats_sk, player_sk, game_sk, date_sk, value)
    SELECT stats_sk, player_sk, game_sk, date_sk, value FROM temp_fact as tf
       INNER JOIN {schema}."Player_Dim" as pd
        ON tf.steam_id = pd.steam_id 
       INNER JOIN {schema}."Game_Dim" as gd
        ON tf.game = gd.name
       INNER JOIN {schema}."Stats_Dim" as sd
        ON tf.name = sd.name
       INNER JOIN {schema}."Date_Dim" as dd
        ON DATE(tf.date) = dd.full_date;
    """
