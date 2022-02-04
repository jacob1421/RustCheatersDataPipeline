BEGIN; -- start transaction

--Create temp table for staged data
CREATE TEMPORARY TABLE temp_dim ON COMMIT DROP AS
SELECT {{ params.columns }} FROM {{ params.schema }}."{{ params.table }}" LIMIT 0;

--Copy data from S3 Bucket to Database
select aws_s3.table_import_from_s3 (
   'temp_dim',
   '{{ params.columns }}',
   '(FORMAT CSV, HEADER)',
   '{{ params.bucket }}',
   '{{ params.bucket_key }}',
   '{{ params.region }}'
);

--Insert data that does not exist in our dimension table
INSERT INTO {{ params.schema }}."{{ params.table }}"({{ params.columns }})
SELECT {{ params.columns }} FROM temp_dim
ON CONFLICT
DO {{ params.conflict_action }};

COMMIT; -- drops the temp table