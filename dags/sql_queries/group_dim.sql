INSERT INTO public."Group_Dim"(group_id)
VALUES (?)
ON CONFLICT(group_id)
DO NOTHING;