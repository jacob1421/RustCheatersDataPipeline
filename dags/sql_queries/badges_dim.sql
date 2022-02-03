INSERT INTO "Badges_Dim"(badge_id, app_id, community_item_id, xp, level)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(badge_id, app_id, community_item_id, level)
DO NOTHING;