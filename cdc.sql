INSERT INTO films VALUES (33, 'Inside Out 3', '2025-04-09', 100, 'PG', '');
INSERT INTO films VALUES (34, 'Cars 4', '2025-04-09', 101, 'PG', '');
UPDATE films SET plot='This is the fourth season of Inside Out' WHERE film='Inside Out 4';
UPDATE films SET run_time=99 WHERE number >= 30;
DELETE FROM films WHERE film='Inside Out 5';

UPDATE films SET run_time=101 WHERE number >= 30;