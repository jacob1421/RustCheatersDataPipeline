INSERT INTO public."Friend_Dim"(steam_id)
VALUES (?)
ON CONFLICT(steam_id)
DO NOTHING;