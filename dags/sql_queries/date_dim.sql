INSERT INTO public."Date_Dim"(full_date, day, month, year, week_day)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(full_date, day, month, year, week_day)
DO NOTHING;