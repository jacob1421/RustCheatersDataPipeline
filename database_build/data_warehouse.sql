BEGIN;

--NEEDED FOR AWS POSTGRES PLUGIN S3 -> Postgres Table
CREATE EXTENSION IF NOT EXISTS aws_commons; 
CREATE EXTENSION IF NOT EXISTS aws_s3;

--Create Schema
DROP SCHEMA IF EXISTS rust_data_warehouse CASCADE;
CREATE SCHEMA IF NOT EXISTS rust_data_warehouse
    AUTHORIZATION jacob1421;

--Create Tables
CREATE TABLE IF NOT EXISTS rust_data_warehouse."Achievement_Fact"
(
    player_sk integer NOT NULL,
    achievement_sk integer NOT NULL,
    game_sk integer NOT NULL,
    date_sk integer NOT NULL,
    "time" time with time zone NOT NULL,
    CONSTRAINT "Achievement_Fact_pkey" PRIMARY KEY (player_sk, achievement_sk, game_sk, date_sk)
);

CREATE TABLE IF NOT EXISTS rust_data_warehouse."Achievements_Dim"
(
    achievement_sk integer NOT NULL DEFAULT nextval('"Achievements_Dim_achievement_sk_seq"'::regclass),
    name text COLLATE pg_catalog."default" NOT NULL,
    description text COLLATE pg_catalog."default",
    CONSTRAINT "Achievements_Dim_pkey" PRIMARY KEY (achievement_sk)
);

CREATE TABLE IF NOT EXISTS rust_data_warehouse."Badges_Dim"
(
    badge_sk integer NOT NULL DEFAULT nextval('"Badges_Dim_badge_sk_seq"'::regclass),
    badge_id bigint NOT NULL,
    app_id bigint,
    community_item_id bigint,
    xp integer NOT NULL,
    level integer NOT NULL,
    CONSTRAINT "Badges_Dim_pkey" PRIMARY KEY (badge_sk)
);

CREATE TABLE IF NOT EXISTS rust_data_warehouse."Badges_Fact"
(
    player_sk integer NOT NULL,
    badge_sk integer NOT NULL,
    date_sk integer NOT NULL,
    scarcity integer NOT NULL,
    steam_level integer NOT NULL DEFAULT 0,
    "time" time with time zone NOT NULL,
    CONSTRAINT "Badges_Fact_pkey" PRIMARY KEY (player_sk, badge_sk, date_sk)
);

CREATE TABLE IF NOT EXISTS rust_data_warehouse."Bans_Fact"
(
    player_sk integer NOT NULL,
    date_sk integer NOT NULL,
    num_vac_bans integer NOT NULL DEFAULT 0,
    num_game_bans integer NOT NULL DEFAULT 0,
    community_banned boolean NOT NULL,
    economy_ban text COLLATE pg_catalog."default" NOT NULL,
    vac_banned boolean NOT NULL,
    CONSTRAINT "Bans_Fact_pkey" PRIMARY KEY (player_sk)
);

CREATE TABLE IF NOT EXISTS rust_data_warehouse."Date_Dim"
(
    date_sk integer NOT NULL DEFAULT nextval('"Date_Dim_date_sk_seq"'::regclass),
    full_date date NOT NULL,
    day integer NOT NULL,
    month integer NOT NULL,
    year integer NOT NULL,
    week_day text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT "Date_Dim_pkey" PRIMARY KEY (date_sk)
);

CREATE TABLE IF NOT EXISTS rust_data_warehouse."Friend_Dim"
(
    friend_sk integer NOT NULL DEFAULT nextval('"Friend_Dim_friend_sk_seq"'::regclass),
    steam_id bigint NOT NULL,
    CONSTRAINT "Friend_Dim_pkey" PRIMARY KEY (friend_sk)
);

CREATE TABLE IF NOT EXISTS rust_data_warehouse."Friends_Fact"
(
    player_sk integer NOT NULL,
    player_friend_sk integer NOT NULL,
    date_sk integer NOT NULL,
    relationship_sk integer NOT NULL,
    "time" time with time zone NOT NULL,
    CONSTRAINT "Friends_Fact_pkey" PRIMARY KEY (player_sk, player_friend_sk, date_sk, relationship_sk)
);

CREATE TABLE IF NOT EXISTS rust_data_warehouse."Game_Dim"
(
    game_sk integer NOT NULL DEFAULT nextval('"Game_Dim_game_sk_seq"'::regclass),
    game_id bigint NOT NULL,
    name text COLLATE pg_catalog."default" NOT NULL,
    has_community_visible_stats boolean NOT NULL,
    CONSTRAINT "Game_Dim_pkey" PRIMARY KEY (game_sk)
);

CREATE TABLE IF NOT EXISTS rust_data_warehouse."Game_Playing_Banned_Fact"
(
    player_sk integer NOT NULL,
    game_sk integer NOT NULL,
    date_sk integer NOT NULL,
    CONSTRAINT "Game_Playing_Banned_Fact_pkey" PRIMARY KEY (player_sk, game_sk)
);

CREATE TABLE IF NOT EXISTS rust_data_warehouse."Game_Playtime_Fact"
(
    player_sk integer NOT NULL,
    game_sk integer NOT NULL,
    date_sk integer NOT NULL,
    playtime_windows_mins bigint NOT NULL DEFAULT 0,
    playtime_mac_mins bigint NOT NULL DEFAULT 0,
    playtime_linux_mins bigint NOT NULL DEFAULT 0,
    playtime_two_weeks bigint NOT NULL DEFAULT 0,
    CONSTRAINT "Game_Playtime_Fact_pkey" PRIMARY KEY (player_sk, game_sk)
);

CREATE TABLE IF NOT EXISTS rust_data_warehouse."Group_Dim"
(
    group_sk integer NOT NULL DEFAULT nextval('"Group_Dim_group_sk_seq"'::regclass),
    group_id bigint NOT NULL,
    CONSTRAINT "Group_Dim_pkey" PRIMARY KEY (group_sk)
);

CREATE TABLE IF NOT EXISTS rust_data_warehouse."Groups_Fact"
(
    player_sk integer NOT NULL,
    group_sk integer NOT NULL,
    date_sk integer NOT NULL,
    CONSTRAINT "Groups_Fact_pkey" PRIMARY KEY (player_sk, group_sk)
);

CREATE TABLE IF NOT EXISTS rust_data_warehouse."Player_Dim"
(
    player_sk integer NOT NULL DEFAULT nextval('"Player_Dim_player_sk_seq"'::regclass),
    steam_id bigint NOT NULL,
    created_at timestamp with time zone,
    community_vis_state integer,
    profile_state integer,
    persona_name text COLLATE pg_catalog."default",
    avatar_hash text COLLATE pg_catalog."default",
    persona_state integer,
    comment_permission integer,
    real_name text COLLATE pg_catalog."default",
    primary_clan_id bigint,
    loc_country_code text COLLATE pg_catalog."default",
    loc_state_code text COLLATE pg_catalog."default",
    loc_city_id integer,
    CONSTRAINT "Player_Dim_pkey" PRIMARY KEY (player_sk)
);

CREATE TABLE IF NOT EXISTS rust_data_warehouse."Relationship_Dim"
(
    relationship_sk integer NOT NULL DEFAULT nextval('"Relationship_Dim_relationship_sk_seq"'::regclass),
    relationship text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT "Relationship_Dim_pkey" PRIMARY KEY (relationship_sk)
);

CREATE TABLE IF NOT EXISTS rust_data_warehouse."Stats_Dim"
(
    stats_sk integer NOT NULL DEFAULT nextval('"Stats_Dim_stats_sk_seq"'::regclass),
    name text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT "Stats_Dim_pkey" PRIMARY KEY (stats_sk)
);

CREATE TABLE IF NOT EXISTS rust_data_warehouse."Stats_Fact"
(
    stats_sk integer NOT NULL,
    player_sk integer NOT NULL,
    game_sk integer NOT NULL,
    date_sk integer NOT NULL,
    value real NOT NULL,
    CONSTRAINT "Stats_Fact_pkey" PRIMARY KEY (stats_sk, player_sk, game_sk)
);

ALTER TABLE IF EXISTS rust_data_warehouse."Achievement_Fact"
    ADD CONSTRAINT "fk_Player_Owned_Games_Fact_Achievement_Dim1" FOREIGN KEY (achievement_sk)
    REFERENCES rust_data_warehouse."Achievements_Dim" (achievement_sk) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS rust_data_warehouse."Achievement_Fact"
    ADD CONSTRAINT "fk_Player_Owned_Games_Fact_Date_Dim1" FOREIGN KEY (date_sk)
    REFERENCES rust_data_warehouse."Date_Dim" (date_sk) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS rust_data_warehouse."Achievement_Fact"
    ADD CONSTRAINT "fk_Player_Owned_Games_Fact_Game_Dim1" FOREIGN KEY (game_sk)
    REFERENCES rust_data_warehouse."Game_Dim" (game_sk) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS rust_data_warehouse."Achievement_Fact"
    ADD CONSTRAINT "fk_Player_Owned_Games_Fact_Player_Dim1" FOREIGN KEY (player_sk)
    REFERENCES rust_data_warehouse."Player_Dim" (player_sk) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS rust_data_warehouse."Badges_Fact"
    ADD CONSTRAINT "fk_Player_Badges_Fact_Badges_Dim1" FOREIGN KEY (badge_sk)
    REFERENCES rust_data_warehouse."Badges_Dim" (badge_sk) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS rust_data_warehouse."Badges_Fact"
    ADD CONSTRAINT "fk_Player_Badges_Fact_Date_Dim1" FOREIGN KEY (date_sk)
    REFERENCES rust_data_warehouse."Date_Dim" (date_sk) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS rust_data_warehouse."Badges_Fact"
    ADD CONSTRAINT "fk_Player_Badges_Fact_Player_Dim1" FOREIGN KEY (player_sk)
    REFERENCES rust_data_warehouse."Player_Dim" (player_sk) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS rust_data_warehouse."Bans_Fact"
    ADD CONSTRAINT "fk_Player_Bans_Fact_Date_Dim1" FOREIGN KEY (date_sk)
    REFERENCES rust_data_warehouse."Date_Dim" (date_sk) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS rust_data_warehouse."Bans_Fact"
    ADD CONSTRAINT "fk_Player_Bans_Fact_Player_Dim1" FOREIGN KEY (player_sk)
    REFERENCES rust_data_warehouse."Player_Dim" (player_sk) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;
CREATE INDEX IF NOT EXISTS "Bans_Fact_pkey"
    ON rust_data_warehouse."Bans_Fact"(player_sk);


ALTER TABLE IF EXISTS rust_data_warehouse."Friends_Fact"
    ADD CONSTRAINT "fk_Player_Friends_Fact_Date_Dim1" FOREIGN KEY (date_sk)
    REFERENCES rust_data_warehouse."Date_Dim" (date_sk) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS rust_data_warehouse."Friends_Fact"
    ADD CONSTRAINT "fk_Player_Friends_Fact_Friend_Dim1" FOREIGN KEY (player_friend_sk)
    REFERENCES rust_data_warehouse."Friend_Dim" (friend_sk) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS rust_data_warehouse."Friends_Fact"
    ADD CONSTRAINT "fk_Player_Friends_Fact_Player_Dim1" FOREIGN KEY (player_sk)
    REFERENCES rust_data_warehouse."Player_Dim" (player_sk) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS rust_data_warehouse."Friends_Fact"
    ADD CONSTRAINT "fk_Player_Friends_Fact_Relationship_Dim1" FOREIGN KEY (relationship_sk)
    REFERENCES rust_data_warehouse."Relationship_Dim" (relationship_sk) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS rust_data_warehouse."Game_Playing_Banned_Fact"
    ADD CONSTRAINT "fk_GamePlayingBan_Date_Date_Dim" FOREIGN KEY (date_sk)
    REFERENCES rust_data_warehouse."Date_Dim" (date_sk) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;


ALTER TABLE IF EXISTS rust_data_warehouse."Game_Playing_Banned_Fact"
    ADD CONSTRAINT "fk_GamePlayingBan_Game_Game_Dim" FOREIGN KEY (game_sk)
    REFERENCES rust_data_warehouse."Game_Dim" (game_sk) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;


ALTER TABLE IF EXISTS rust_data_warehouse."Game_Playing_Banned_Fact"
    ADD CONSTRAINT "fk_GamePlayingBan_Player_Player_Dim" FOREIGN KEY (player_sk)
    REFERENCES rust_data_warehouse."Player_Dim" (player_sk) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;


ALTER TABLE IF EXISTS rust_data_warehouse."Game_Playtime_Fact"
    ADD CONSTRAINT "fk_Player_Owned_Games_Fact_Date_Dim1" FOREIGN KEY (date_sk)
    REFERENCES rust_data_warehouse."Date_Dim" (date_sk) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS rust_data_warehouse."Game_Playtime_Fact"
    ADD CONSTRAINT "fk_Player_Owned_Games_Fact_Game_Dim1" FOREIGN KEY (game_sk)
    REFERENCES rust_data_warehouse."Game_Dim" (game_sk) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS rust_data_warehouse."Game_Playtime_Fact"
    ADD CONSTRAINT "fk_Player_Owned_Games_Fact_Player_Dim1" FOREIGN KEY (player_sk)
    REFERENCES rust_data_warehouse."Player_Dim" (player_sk) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS rust_data_warehouse."Groups_Fact"
    ADD CONSTRAINT "fk_Player_Groups_Fact_Date_Dim1" FOREIGN KEY (date_sk)
    REFERENCES rust_data_warehouse."Date_Dim" (date_sk) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS rust_data_warehouse."Groups_Fact"
    ADD CONSTRAINT "fk_Player_Groups_Fact_Groups_Dim1" FOREIGN KEY (group_sk)
    REFERENCES rust_data_warehouse."Group_Dim" (group_sk) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS rust_data_warehouse."Groups_Fact"
    ADD CONSTRAINT "fk_Player_Groups_Fact_Player_Dim1" FOREIGN KEY (player_sk)
    REFERENCES rust_data_warehouse."Player_Dim" (player_sk) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS rust_data_warehouse."Stats_Fact"
    ADD CONSTRAINT "fk_Player_Stats_Fact_Date_Dim1" FOREIGN KEY (date_sk)
    REFERENCES rust_data_warehouse."Date_Dim" (date_sk) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS rust_data_warehouse."Stats_Fact"
    ADD CONSTRAINT "fk_Player_Stats_Fact_Game_Dim1" FOREIGN KEY (game_sk)
    REFERENCES rust_data_warehouse."Game_Dim" (game_sk) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS rust_data_warehouse."Stats_Fact"
    ADD CONSTRAINT "fk_Player_Stats_Fact_Player_Dim1" FOREIGN KEY (player_sk)
    REFERENCES rust_data_warehouse."Player_Dim" (player_sk) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS rust_data_warehouse."Stats_Fact"
    ADD CONSTRAINT "fk_Player_Stats_Fact_Stats_Dim1" FOREIGN KEY (stats_sk)
    REFERENCES rust_data_warehouse."Stats_Dim" (stats_sk) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

END;

END;