\set ON_ERROR_STOP on

CREATE TABLE stagec_psql (id INT, big BIGINT, score FLOAT8, name TEXT);
INSERT INTO stagec_psql VALUES (1, 10, 1.25, 'one'), (2, 20, -2.5, NULL);
SELECT id, big, score, name FROM stagec_psql ORDER BY id;

SELECT id, name FROM stagec_psql WHERE id = $1 \parse stagec_psql_select
\bind_named stagec_psql_select 1 \g

UPDATE stagec_psql SET name = 'updated' WHERE id = 1;
DELETE FROM stagec_psql WHERE id = 2;
SELECT id, name FROM stagec_psql;

BEGIN;
INSERT INTO stagec_psql VALUES (3, 30, 0.0, 'rollback');
ROLLBACK;
SELECT id FROM stagec_psql;

CREATE TABLE stagec_psql_ann (id INT, v VECTOR(2));
CREATE INDEX stagec_psql_ann_idx ON stagec_psql_ann USING hnsw (v vector_l2_ops);
INSERT INTO stagec_psql_ann VALUES (1, [1,0]), (2, [0,1]);
SELECT id FROM stagec_psql_ann ORDER BY v <-> $1 ASC LIMIT 1 \parse stagec_psql_ann_query
\bind_named stagec_psql_ann_query '[1,0]' \g

\close_prepared stagec_psql_select
\close_prepared stagec_psql_ann_query
