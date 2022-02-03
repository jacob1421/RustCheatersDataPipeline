INSERT INTO "Achievements_Dim"(name, description)
VALUES(?,?)
ON CONFLICT(name)
DO NOTHING;