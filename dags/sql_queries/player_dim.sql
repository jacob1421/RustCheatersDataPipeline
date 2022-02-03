INSERT INTO public."Player_Dim"(steam_id, created_at, community_vis_state, profile_state, persona_name, avatar_hash, persona_state, comment_permission, real_name, primary_clan_id, loc_country_code, loc_state_code, loc_city_id)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(steam_id)
DO
    --Update Profile
    UPDATE SET community_vis_state = EXCLUDED.community_vis_state,
    profile_state = EXCLUDED.profile_state,
    persona_name = EXCLUDED.persona_name,
    avatar_hash = EXCLUDED.avatar_hash,
    persona_state = EXCLUDED.persona_state,
    comment_permission = EXCLUDED.comment_permission,
    real_name = EXCLUDED.real_name,
    primary_clan_id = EXCLUDED.primary_clan_id,
    loc_country_code = EXCLUDED.loc_country_code,
    loc_state_code = EXCLUDED.loc_state_code,
    loc_city_id = EXCLUDED.loc_city_id;
