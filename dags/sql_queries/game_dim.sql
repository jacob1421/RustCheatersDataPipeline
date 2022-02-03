INSERT INTO public."Game_Dim"(game_id, name, has_community_visible_stats)
VALUES (?, ?, ?)
ON CONFLICT(game_id)
DO NOTHING;
