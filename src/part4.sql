---- EX 1
-- CREATE TABLE test1(
-- 	Nickname VARCHAR NOT NULL PRIMARY KEY ,
--     Birthday date NOT NULL
-- );
-- CREATE TABLE test2(
-- 	Nickname1 VARCHAR NOT NULL PRIMARY KEY ,
--     Birthday1 date NOT NULL
-- );
-- SELECT tablename
--   FROM pg_catalog.pg_tables
--   WHERE schemaname  = 'public'
--   and tablename LIKE 'test%'

CREATE OR REPLACE PROCEDURE delete_table(begin_name text)
    AS $$
    DECLARE table_rec record;
    BEGIN
         for table_rec in (SELECT tablename
                            FROM pg_catalog.pg_tables
                            WHERE schemaname  = 'public' and
                            tablename LIKE 'test%')
         loop
             execute 'drop table '||table_rec.tablename||' cascade';
         end loop;
    END;
    $$
    LANGUAGE plpgsql;
-- CALL delete_table('test');

----   EX 2
CREATE OR REPLACE PROCEDURE get_num_of_func_and_procedure(OUT num int)
    AS $$
    DECLARE
        rec record;
        res int = 0;
    BEGIN
        FOR rec IN (SELECT
--                         count(r.routine_name) OVER(ORDER BY r.routine_name),
                        r.routine_name
                            || ' ' ||  string_agg(p.parameter_name, ',')
                            || ' ' || string_agg(p.data_type, ',')AS info
            FROM information_schema.routines AS r
            INNER JOIN information_schema.parameters AS p
            ON r.specific_name = p.specific_name
            WHERE r.specific_schema = 'public'
            GROUP BY routine_name)
        loop
--             RAISE INFO '% %' , rec.count, rec.info;
            RAISE INFO '%' , rec.info;
            res := res + 1;
        end loop;
        num := res;
    END
    $$
    LANGUAGE plpgsql;
BEGIN;
-- CALL get_num_of_func_and_procedure(0);
-- END;

---- EX 3

CREATE OR REPLACE PROCEDURE delete_DML_triggers(OUT count_triggers int)
    AS $$
    DECLARE
    table_rec record;
    BEGIN
        SELECT count(trigger_name)
        INTO count_triggers
        FROM (SELECT trigger_name, event_object_table
                            FROM information_schema.triggers
                            WHERE event_manipulation = 'INSERT'
                            or event_manipulation = 'DELETE'
                            or event_manipulation = 'UPDATE'
                            or event_manipulation = 'SELECT') as temp;
        for table_rec in (SELECT trigger_name, event_object_table
                            FROM information_schema.triggers
                            WHERE event_manipulation = 'INSERT'
                            or event_manipulation = 'DELETE'
                            or event_manipulation = 'UPDATE'
                            or event_manipulation = 'SELECT')
        loop
             execute 'drop trigger '||table_rec.trigger_name||
             ' ON '||table_rec.event_object_table||
             ' CASCADE';
        end loop;
    END;
    $$
    LANGUAGE plpgsql;
-- SELECT * FROM information_schema.triggers;
-- CALL delete_DML_triggers(0);
-- SELECT * FROM information_schema.triggers;

---- EX 4

CREATE OR REPLACE PROCEDURE get_info_about_func_or_procedure(IN str VARCHAR)
    AS $$
    DECLARE rec record;
    BEGIN
        FOR rec IN
        (SELECT r.routine_name AS name, r.routine_definition AS def
        FROM information_schema.routines r
        WHERE r.specific_schema = 'public'
        AND (r.routine_type = 'FUNCTION' OR r.routine_type = 'PROCEDURE')
        AND routine_definition LIKE '%' || str || '%')
        loop
            RAISE INFO '% % ', rec.name, rec.def;
        end loop;
    END
    $$
    LANGUAGE plpgsql;
-- BEGIN;
-- CALL get_info_about_func_or_procedure('RETURN');
-- END;