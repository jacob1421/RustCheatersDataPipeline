INSERT INTO public."Stats_Dim"(name)
ON CONFLICT(name)
DO NOTHING;