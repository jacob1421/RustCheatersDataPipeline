INSERT INTO public."Relationship_Dim"(relationship)
VALUES (?)
ON CONFLICT(relationship)
DO NOTHING;