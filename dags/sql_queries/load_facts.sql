SELECT player_sk, achievement_sk, game_sk, date_sk
	FROM rust_data_warehouse."Achievement_Fact";

--Create temp table for staged data
CREATE TEMPORARY TABLE temp_dim (
	steam_id bigint,
	name text,
	game_name text,
	unlock_ts timestamp with time zone
)ON COMMIT DROP;

--Copy data from S3 Bucket to Database
select aws_s3.table_import_from_s3 (
   'temp_dim',
   'steam_id, name, game_name, unlock_ts',
   '(FORMAT CSV, HEADER)',
   'rust-cheaters',
   '/data-lake/staged/steam/achievement_fact/2022/02/04/2022-02-04T06_13_35Z_to_2022-02-04T07_13_35Z.csv',
   'us-east-1'
);

-- SELECT player_sk, achievement_sk, game_sk, date_sk, td.unlock_ts::time as time  FROM temp_dim as td
-- INNER JOIN rust_data_warehouse."Achievements_Dim" as ad
-- 	ON td.name = ad.name
-- INNER JOIN rust_data_warehouse."Player_Dim" as pd
-- 	ON td.steam_id = pd.steam_id
-- INNER JOIN rust_data_warehouse."Game_Dim" as gd
-- 	ON td.game_name = gd.name
-- INNER JOIN rust_data_warehouse."Date_Dim" as dd
--     ON DATE(td.unlock_ts) = dd.full_date;

-- Insert data that does not exist in our dimension table
INSERT INTO rust_data_warehouse."Achievement_Fact"(player_sk, achievement_sk, game_sk, date_sk, time)
SELECT player_sk, achievement_sk, game_sk, date_sk, td.unlock_ts::time WITH TIME ZONE as time  FROM temp_dim as td
INNER JOIN rust_data_warehouse."Achievements_Dim" as ad
	ON td.name = ad.name
INNER JOIN rust_data_warehouse."Player_Dim" as pd
	ON td.steam_id = pd.steam_id
INNER JOIN rust_data_warehouse."Game_Dim" as gd
	ON td.game_name = gd.name
INNER JOIN rust_data_warehouse."Date_Dim" as dd
    ON DATE(td.unlock_ts) = dd.full_date;


