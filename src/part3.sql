---- EX 1

CREATE OR REPLACE FUNCTION get_readable_transferredpoints()
    RETURNS TABLE (Peer1 varchar, Peer2 varchar, Pointsamount numeric) AS $$
    BEGIN
         RETURN QUERY (
         WITH all_uniq__sum_points_pairs AS (
             (SELECT checkingpeer AS peer, checkedpeer AS two, t1.pointsamount
             FROM transferredpoints t1 WHERE checkedpeer > checkingpeer)
             UNION
             (SELECT checkedpeer AS peer , checkingpeer AS two , -t2.pointsamount
             FROM transferredpoints t2 where checkingpeer > checkedpeer)
             )
         SELECT peer AS Peer1, two AS Peer2, SUM(a.pointsamount) AS Pointsamount
         FROM all_uniq__sum_points_pairs a GROUP BY Peer1, Peer2);
        RETURN;
    END
    $$
    LANGUAGE plpgsql;
-- SELECT * FROM get_readable_transferredpoints();

---- EX 2

CREATE OR REPLACE FUNCTION get_peer_task_xp_table()
	RETURNS TABLE(peer VARCHAR, task VARCHAR, xp bigint) AS $$
	BEGIN
		RETURN QUERY (
			SELECT c.peer, c.task, xp.xpamount AS xp FROM checks c
			INNER JOIN p2p ON c.id = check_id and state = 'Success'
			INNER JOIN xp ON xp.checkid = c.id
		);
	RETURN;
	END
	$$
	LANGUAGE plpgsql;
-- SELECT * FROM get_peer_task_xp_table();

---- EX 3

CREATE OR REPLACE FUNCTION get_peer_no_exit_for_in_day(day date)
    RETURNS TABLE (Peers VARCHAR) AS $$
    BEGIN
        RETURN QUERY (
			SELECT DISTINCT peer AS Peers 
			FROM timetracking WHERE timetracking.Date = day
        	GROUP BY peer having (SUM(state) < 4)
		);
    END
    $$
    LANGUAGE plpgsql;
-- SELECT * FROM get_peer_no_exit_for_in_day('2023-01-15');

-- EX 4

CREATE OR REPLACE PROCEDURE get_percent_success_and_fail_checks(
	c refcursor = 'curs')
	AS $$
	BEGIN
		OPEN c FOR
		WITH count_all_checks AS 
			(SELECT COUNT(c.id) AS all_c FROM checks c
			INNER JOIN p2p ON c.id = p2p.check_id and state <> 'Start')
			, success AS 
			(SELECT COUNT(c.id) AS all_suc FROM checks c
			  INNER JOIN p2p ON c.id = p2p.check_id and state = 'Success')
			  
		SELECT 
			((success.all_suc::numeric / count_all_checks.all_c) * 100)::int AS SuccessfulChecks
			, (100 - (success.all_suc::numeric / count_all_checks.all_c) * 100)::int AS UnsuccessfulChecks
		FROM count_all_checks, success;
	END
	$$
	LANGUAGE plpgsql;
	-- BEGIN;
	-- CALL get_percent_success_and_fail_checks();
	-- FETCH ALL FROM "curs";
	-- END;
      
---- EX 5

CREATE OR REPLACE PROCEDURE get_pointschange_in_peer(
	c refcursor = 'curs')
	AS $$
	BEGIN
		OPEN c FOR
		SELECT checkingpeer AS Peer
			 , SUM(all_sum.sum) AS PointsChange 
		FROM
			(SELECT checkingpeer
			 		, SUM(pointsamount) AS sum
			FROM transferredpoints 
			GROUP BY checkingpeer
			 
			UNION
			 
			SELECT checkedpeer
			 	   , -SUM(pointsamount) AS sum
			FROM transferredpoints 
			GROUP BY checkedpeer) AS all_sum 
		GROUP BY checkingpeer 
		ORDER BY PointsChange DESC;
	END
	$$
	LANGUAGE plpgsql;
	-- BEGIN;
	-- CALL get_pointschange_in_peer();
	-- FETCH ALL FROM "curs";
	-- END;

---- EX 6

CREATE OR REPLACE PROCEDURE get_pointschange_in_peer_from_ex_1(
	c refcursor = 'curs')
	AS $$
	BEGIN
		OPEN c FOR
			WITH readable_tp AS 
			(SELECT * FROM get_readable_transferredpoints())
			
			SELECT Peer
				   , SUM(pointsamount) AS PointsChange
			FROM
				(SELECT peer1 AS peer
						, pointsamount 
				FROM readable_tp

				UNION ALL

				SELECT peer2 AS peer
					, -pointsamount 
				FROM readable_tp) AS all_peer_points
			GROUP BY peer ORDER BY PointsChange DESC;
	END
	$$
	LANGUAGE plpgsql;
	-- BEGIN;
	-- CALL get_pointschange_in_peer_from_ex_1();
	-- FETCH ALL FROM "curs";
	-- END;

---- EX 7

CREATE OR REPLACE PROCEDURE get_the_most_repeated_task_in_day(
	c refcursor = 'curs')
	AS $$
	BEGIN
		OPEN c FOR
			SELECT to_char(date, 'dd.mm.yyyy') AS Day
		   		   , substring(task from '(.*?)_') AS Task
		   FROM
			(SELECT COUNT(task) AS sum
					, task, date
			FROM checks 
			GROUP BY task, date) AS counts_tasks
		   GROUP BY date, task ORDER BY Day;
	END
	$$
	LANGUAGE plpgsql;
	-- BEGIN;
	-- CALL get_the_most_repeated_task_in_day();
	-- FETCH ALL FROM "curs";
	-- END;

----EX 8

CREATE OR REPLACE PROCEDURE get_duration_of_last_check(
	c refcursor = 'curs')
	AS $$
	BEGIN
		OPEN c FOR
		SELECT (MAX(time) - MIN(time))::time(0) AS duration_of_last_check 
		FROM p2p
		WHERE check_id = 
			(SELECT check_id FROM p2p
			WHERE id = (SELECT MAX(id) 
						FROM p2p 
						WHERE state <> 'Start'));
	END
	$$
	LANGUAGE plpgsql;
	-- BEGIN;
	-- CALL get_duration_of_last_check();
	-- FETCH ALL FROM "curs";
	-- END;

---- EX 9

CREATE OR REPLACE PROCEDURE get_peers_done_block(
block text
, c refcursor = 'curs')
    AS $$
    BEGIN
        OPEN c FOR
			WITH peer_success AS 
			(SELECT DISTINCT peer
							 , c.task
							 , date 
			FROM p2p p
			INNER JOIN checks c ON c.id = p.check_id
			INNER JOIN tasks t ON substring(c.task from '(.*?)[0-9]*_') = 
								  substring(t.title from '(.*?)[0-9]*_')
							   AND substring(t.title from '(.*?)[0-9]*_') = block
			WHERE state = 'Success')
			
			SELECT peer AS Peer
				   , to_char(MAX(date)
			       , 'dd.mm.yyyy') AS Day
			FROM peer_success 
			GROUP BY peer HAVING COUNT(DISTINCT task) = (
				WITH tasks_in_block AS 
				(SELECT DISTINCT substring(title from '(.*?)_') AS task
				FROM tasks
				WHERE  substring(title from '(.*?)[0-9]*_') = block)
				
				SELECT COUNT(*) 
				FROM tasks_in_block);
    END
    $$
    LANGUAGE plpgsql;
-- BEGIN;
-- CALL get_peers_done_block('D');
-- FETCH ALL FROM "curs";
-- END;

---- EX 10

CREATE OR REPLACE PROCEDURE get_to_whom_to_go_for_check(
	c refcursor = 'curs')
	AS $$
	BEGIN
		OPEN c FOR
			WITH all_recomendations_sum AS
		(SELECT peer
			, recommendedpeer
			, SUM(count_) AS sum_c
		FROM
			(SELECT peer
					, recommendedpeer
					, 1 AS count_
			FROM
				(SELECT peer1 AS peer
						, recommendedpeer 
				FROM friends f
				INNER JOIN recommendations r ON f.peer2 = r.peer
				WHERE peer1 <> recommendedpeer

				UNION ALL

				SELECT peer2 AS peer, recommendedpeer FROM friends f
				INNER JOIN recommendations r ON f.peer1 = r.peer
				WHERE peer2 <> recommendedpeer) AS all_recommended
			ORDER BY 1) AS temp
		GROUP BY peer, recommendedpeer)
		, max_sums_in_recomendations AS
		(SELECT peer
	 		   , MAX(sum_c) AS max_sum
		FROM all_recomendations_sum 
		GROUP BY peer)

	SELECT DISTINCT a.peer, a.recommendedpeer
	FROM max_sums_in_recomendations m
		,  all_recomendations_sum a
		WHERE m.max_sum = a.sum_c
		AND a.peer =  m.peer;
	END
	$$
	LANGUAGE plpgsql;
	-- BEGIN;
	-- CALL get_to_whom_to_go_for_check();
	-- FETCH ALL FROM "curs";
	-- END;

-- EX 11

CREATE OR REPLACE PROCEDURE get_percents_of_peers(
block1 text
, block2 text
, c refcursor = 'curs')
    AS $$
    BEGIN
    OPEN c FOR
		WITH one AS(
		-- done only block 1
		(WITH count_current_peers AS 
		(SELECT COUNT(peer) AS peer
		FROM (SELECT DISTINCT peer 
			  FROM checks 
			  WHERE substring(task from '(.*?)[0-9]*_') = block1

			  EXCEPT

			  SELECT DISTINCT peer 
			  FROM checks 
			  WHERE substring(task from '(.*?)[0-9]*_') = block2) AS temp)

		SELECT ((count_current_peers.peer::numeric  / count_all_peers.peer) * 100)::int AS StartedBlock1
		FROM count_current_peers,
			(SELECT COUNT(nickname) AS peer FROM peers)AS count_all_peers)
		)
		, two AS(
		-- done only block 2
			WITH current_peers AS 
			(SELECT COUNT(peer) AS peer
			FROM 
				(SELECT DISTINCT peer 
				 FROM checks 
				 WHERE substring(task from '(.*?)[0-9]*_') = block2

				 EXCEPT

				 SELECT DISTINCT peer 
				 FROM checks 
				 WHERE substring(task from '(.*?)[0-9]*_') = block1) AS temp)

			SELECT((current_peers.peer::numeric / count_all_peers.peer) * 100)::int AS StartedBlock2 
			FROM current_peers,
			(SELECT COUNT(nickname) AS peer FROM peers)AS count_all_peers)
		, three AS    
		-- done block2 and block 1
		(WITH count_hard_word_peers AS 
		 (SELECT COUNT(peer) 
		  FROM
			 (SELECT DISTINCT peer 
			  FROM checks WHERE substring(task from '(.*?)[0-9]*_') = block1
			 INTERSECT
			 SELECT DISTINCT peer 
			 FROM checks 
			  WHERE substring(task from '(.*?)[0-9]*_') = block2) AS temp)

			 SELECT (((SELECT * FROM count_hard_word_peers)::numeric / count_all_peers.peer) *
						100)::int AS StartedBothBlocks
			 FROM count_hard_word_peers,
			 (SELECT COUNT(nickname) AS peer FROM peers) AS count_all_peers)
			, four AS    
			-- NO done block2 and block 1
			(WITH count_no_work_peer AS 
			 (SELECT  COUNT(peer) 
			 FROM (SELECT DISTINCT nickname AS peer 
				  FROM (SELECT DISTINCT peer 
						 FROM checks 
						 WHERE substring(task from '(.*?)[0-9]*_') = block1

						 UNION

						 SELECT DISTINCT peer 
						 FROM checks 
						 WHERE substring(task from '(.*?)[0-9]*_') = block2)AS  all_work_peers
						 RIGHT JOIN peers ON nickname = peer
						 WHERE peer is NULL) AS no_worl_peers
			)

			SELECT (((SELECT * FROM count_no_work_peer)::numeric / count_all_peers.peer) * 
					100)::int AS DidntStartAnyBlock
			FROM count_no_work_peer,
						   (SELECT COUNT(nickname) AS peer FROM peers) AS count_all_peers)

		SELECT one.StartedBlock1
			   ,two.StartedBlock2
			   , three.StartedBothBlocks
			   , four.DidntStartAnyBlock
		FROM one,
			 two,
			 three,
			 four;
    END
    $$
    LANGUAGE plpgsql;
-- BEGIN;
-- CALL get_percents_of_peers('C', 'D');
-- FETCH ALL FROM "curs";
-- END;

---- EX 12

CREATE OR REPLACE PROCEDURE most_num_of_friends(
	N int
	, c refcursor = 'curs')
	AS $$
	BEGIN
	OPEN c FOR
		SELECT nickname AS peer
		, SUM(sum_fr) AS FriendsCount 
		FROM
			(SELECT nickname
			 		, 0 AS sum_fr
				FROM peers
			 
			UNION ALL
			 
			SELECT nickname
			 	   , COUNT(peer1) AS sum_fr
				FROM peers
				INNER JOIN friends ON peer1 = nickname
				GROUP BY nickname 
			 
			UNION ALL
			 
			SELECT nickname
			 	   , COUNT(peer2) AS sum_fr
				FROM peers
				INNER JOIN friends ON peer2 = nickname
				GROUP BY nickname ) AS all_sum_fr
		GROUP BY nickname ORDER BY FriendsCount DESC LIMIT N;

	END
	$$
	LANGUAGE plpgsql;
-- BEGIN;
-- CALL most_num_of_friends(5);
-- FETCH ALL FROM "curs";
-- END;

---- EX 13

CREATE OR REPLACE PROCEDURE  get_percent_success_and_fail_checks_in_birthday(
	c refcursor = 'curs')
	AS $$
	BEGIN
		OPEN c FOR
			WITH count_all_peers AS 
			(SELECT COUNT(nickname) AS res
			FROM peers)
			, Suc AS
			(SELECT COUNT(DISTINCT nickname) 
			FROM peers
			INNER JOIN checks c on to_char(birthday, 'dd.mm') = to_char(c.date, 'dd.mm')
			INNER JOIN p2p ON p2p.check_id = c.id
			WHERE p2p.state = 'Success')
			, Fail AS
			(SELECT COUNT(DISTINCT nickname) FROM peers
			INNER JOIN checks c on to_char(birthday, 'dd.mm') = to_char(c.date, 'dd.mm')
			INNER JOIN p2p ON p2p.check_id = c.id
			WHERE p2p.state = 'Failure')

			SELECT ((Suc.count::numeric / (SELECT res 
												  FROM count_all_peers)) * 100)::int
			AS SuccessfulChecks,
				   ((Fail.count::numeric / (SELECT res 
										   FROM count_all_peers)) * 100)::int
			AS UnsuccessfulChecks
			FROM Suc, Fail;
	END
	$$
	LANGUAGE plpgsql;
-- 	BEGIN;
-- 	CALL get_percent_success_and_fail_checks_in_birthday();
-- 	FETCH ALL FROM "curs";
-- 	END;

---- EX 14

CREATE OR REPLACE PROCEDURE get_all_xp_of_peers(
	c refcursor = 'curs')
	AS $$
	BEGIN
		OPEN c FOR
			SELECT peer
				   , SUM(max_xp) AS sum_xp 
			FROM 
				(SELECT peer
						, task
						, MAX(xpamount) AS max_xp 
				FROM xp
				INNER JOIN checks c on xp.checkid = c.id
				INNER JOIN peers p on c.peer = p.nickname 
				GROUP BY task, peer) AS max_xp
			GROUP BY peer;
	END
	$$
	LANGUAGE plpgsql;
-- 	BEGIN;
-- 	CALL get_all_xp_of_peers();
-- 	FETCH ALL FROM "curs";
-- 	END;
	
---- EX 15

CREATE OR REPLACE PROCEDURE get_peers_done_two_in_three_task(
    one_task VARCHAR
    , two_task VARCHAR
    , three_task VARCHAR
    , c refcursor = 'curs')
    AS $$
    BEGIN
    OPEN c FOR (SELECT DISTINCT peer 
				FROM checks
                INNER JOIN p2p p ON checks.id = p.check_id
                WHERE task = one_task AND state = 'Success'
				
                INTERSECT
				
                SELECT DISTINCT peer 
				FROM checks
                INNER JOIN p2p p ON checks.id = p.check_id
                WHERE task = two_task AND state = 'Success')
				
                INTERSECT
				
                (SELECT p.nickname 
				FROM peers p
                LEFT OUTER JOIN
					(SELECT DISTINCT nickname 
					FROM peers
					INNER JOIN checks c ON nickname = c.peer
					INNER JOIN p2p p ON c.id = p.check_id
					WHERE task = three_task and state ='Success') AS Suc
                ON Suc.nickname = p.nickname 
				WHERE Suc.nickname is NULL);
    END
    $$
    LANGUAGE plpgsql;
-- BEGIN;
-- CALL get_peers_done_two_in_three_task('C2_SimpleBashUtils', 'D01_Linux', 'C4_s21_math');
-- FETCH ALL FROM "curs";
-- END;

---- EX 16

CREATE OR REPLACE PROCEDURE get_prevtask_counts(
	c refcursor = 'curs')
	AS $$
	BEGIN
		OPEN c FOR
			WITH RECURSIVE prev_task AS (
			SELECT title, parenttask, 0 AS count_t
			FROM tasks
			WHERE parenttask IS NULL

			UNION ALL

			SELECT t.title, t.parenttask, p.count_t + 1
			FROM tasks t
			INNER JOIN prev_task p ON p.title = t.parenttask
			WHERE t.parenttask IS NOT NULL
			)

			SELECT substring(title from '(.*?)_') AS task
				   , count_t AS PrevCount
			FROM prev_task
			ORDER BY task ;
	END
	$$
	LANGUAGE plpgsql;
-- 	BEGIN;
-- 	CALL get_prevtask_counts();
-- 	FETCH ALL FROM "curs";
-- 	END;

---- EX 17

CREATE OR REPLACE PROCEDURE get_licky_day_for_check(
    N int
    , c refcursor = 'curs'
)
    AS $$
    DECLARE
        rec record;
        count_success int = 0;
    BEGIN
       CREATE TEMPORARY TABLE temp_table
							(date DATE)
							ON COMMIT DROP;
        FOR rec IN (
			WITH state_without_start AS(
				SELECT check_id
						, date
						, time
						, state
						, xpamount AS xp
						, maxxp
                FROM checks c
                INNER JOIN p2p ON p2p.check_id = c.id
                INNER JOIN xp x ON x.checkid = c.id
                INNER JOIN tasks t ON t.title = c.task
                WHERE state <> 'Start'
                ORDER BY date, time)
			, state_start AS 
			(SELECT check_id
					, date
					, time
					, state
					, xpamount AS xp
					, maxxp
			FROM checks c
			INNER JOIN p2p ON p2p.check_id = c.id
			INNER JOIN xp x ON x.checkid = c.id
			INNER JOIN tasks t ON t.title = c.task
			WHERE state = 'Start'
			ORDER BY date, time)
			
			SELECT state_without_start.check_id AS check_id
					, state_without_start.date AS date
					, state_start.time AS time
					, state_without_start.state AS state
					,  state_without_start.xp AS xp
					, state_without_start.maxxp AS maxp
					, row_number() OVER
					(PARTITION BY state_without_start.date
					ORDER BY state_without_start.date, state_start.time)
					AS row_number
					FROM state_without_start
					INNER JOIN state_start
					ON state_without_start.check_id = state_start.check_id
					WHERE (state_without_start.xp::numeric /
						   state_without_start.maxxp) * 100 >= 80)
            loop
			IF rec.row_number = 1
			THEN
			count_success := 0;
			END IF;
            IF rec.state = 'Success' THEN count_success := count_success + 1;
				IF count_success >= N THEN
				INSERT INTO temp_table(date) VALUES (rec.date);
                END IF;
            ELSE
                count_success := 0;
            END IF;
            end loop;
        OPEN c FOR SELECT DISTINCT date FROM temp_table;
    END
    $$
    LANGUAGE plpgsql;
-- BEGIN;
-- CALL get_licky_day_for_check(1);
-- FETCH ALL FROM "curs";
-- END;



---- EX 18

CREATE OR REPLACE PROCEDURE get_peer_with_max_completed_task(
	c refcursor = 'curs')
	AS $$
	BEGIN
		OPEN c FOR
			WITH all_count_task AS(
			SELECT peer, COUNT(DISTINCT task) AS num_of_task 
				FROM checks c
				INNER JOIN p2p ON c.id = check_id 
							   AND p2p.state = 'Success'
				GROUP BY peer)

			SELECT peer, num_of_task 
			FROM all_count_task
			WHERE num_of_task = (SELECT MAX(num_of_task) 
								 FROM all_count_task);
	END
	$$
	LANGUAGE plpgsql;
-- 	BEGIN;
-- 	CALL get_peer_with_max_completed_task();
-- 	FETCH ALL FROM "curs";
-- 	END;

---- EX 19

CREATE OR REPLACE PROCEDURE get_peer_with_the_biggest_xp(
	c refcursor = 'curs')
	AS $$
	BEGIN
		OPEN c FOR
			WITH sum_max_xp AS 
			(SELECT peer, SUM(max_xp) AS sum_xp 
			FROM 
				(SELECT peer, task, MAX(xpamount) AS max_xp 
				FROM xp
				INNER JOIN checks c ON xp.checkid = c.id
				INNER JOIN peers p ON c.peer = p.nickname 
				GROUP BY task, peer) AS max_xp
			GROUP BY peer)

			SELECT peer, sum_xp
			FROM sum_max_xp
			WHERE sum_xp = (SELECT MAX(sum_xp) 
							FROM sum_max_xp);
	END
	$$
	LANGUAGE plpgsql;
-- 	BEGIN;
-- 	CALL get_peer_with_the_biggest_xp();
-- 	FETCH ALL FROM "curs";
-- 	END;

---- EX 20

-- 	DROP TABLE IF EXISTS TimeTracking CASCADE;
-- CREATE TABLE TimeTracking (
--     ID BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY
--     , Peer VARCHAR NOT NULL REFERENCES Peers(Nickname)
--     , Date date NOT NULL
--     , Time time WITHOUT TIME ZONE NOT NULL
--     , State int CHECK (State = 1 OR State = 2)
-- );

-- insert into TimeTracking(peer, date, time, state) values
--     ('werewolf', current_date, '10:00', 1),
--     ('werewolf', current_date, '13:59', 2),
--     ('cresswec', current_date, '19:00', 1),
--     ('cresswec', current_date, '19:01', 2),
--     ('mbulwer', current_date, '12:00', 1),
--     ('mbulwer', current_date, '15:00', 2),
--     ('mbulwer', current_date, '19:15', 1),
--     ('mbulwer', current_date, '21:15', 2),
--     ('mbulwer', '2023-07-31', '12:00', 1),
--     ('mbulwer', '2023-07-31', '21:15', 2);

CREATE OR REPLACE PROCEDURE get_peer_who_have_long_time_in_campus_today(
	c refcursor = 'curs')
	AS $$
	BEGIN
		OPEN c FOR
			WITH current_interval_peers_today AS(
			SELECT peer, SUM(time) sum_interval
			FROM
				(
				WITH who_here_now AS 
				(SELECT peer, sum FROM (
					SELECT peer, SUM(count) FROM
						(SELECT peer, COUNT(state) 
						FROM timetracking
						WHERE state = 1 AND date = current_date
						GROUP BY peer

						UNION 

						SELECT peer, -COUNT(state) 
						FROM timetracking
						WHERE state = 2 AND date = current_date
						GROUP BY peer
						) AS all_state_sum
					GROUP BY peer
				) AS sum_state_zero)

				SELECT peer, -(-current_time(0)::time) AS time
					FROM who_here_now
					WHERE sum <> 0

				UNION

				(SELECT peer, -time AS time 
					FROM timetracking 
					WHERE date = current_date AND state = 1

				UNION ALL

				SELECT peer, -(-time) AS time 
				FROM timetracking 
				WHERE date = current_date AND state = 2)
				) AS all_time
			GROUP BY peer)

			SELECT peer
			FROM current_interval_peers_today
			WHERE sum_interval = 
				(SELECT MAX(sum_interval) 
				FROM current_interval_peers_today);
	END
	$$
	LANGUAGE plpgsql;
-- 	BEGIN;
-- 	CALL get_peer_who_have_long_time_in_campus_today();
-- 	FETCH ALL FROM "curs";
-- 	END;

---- EX 21

CREATE OR REPLACE PROCEDURE get_early_peers(
    N int
    , times time
    , c refcursor = 'curs')
	AS
	$$
	BEGIN
	OPEN c FOR 
	SELECT peer FROM timetracking
	WHERE state = 1
	AND time <= times
	GROUP BY peer, time having COUNT(state) >= N;
	END
	$$
	LANGUAGE plpgsql;
-- BEGIN;
-- CALL get_early_peers(1, '14:00:00'::time);
-- FETCH ALL FROM "curs";
-- END;

---- Ex 22

CREATE OR REPLACE PROCEDURE get_peers_exit(
	N int
	, M int
	, c refcursor = 'curs')
    AS $$
    BEGIN
    OPEN c FOR
		SELECT peer
		FROM
			(SELECT peer
			 		, state
			 		, date 
			FROM timetracking
			WHERE date > current_date - N 
			AND state = 2) AS temp
		GROUP BY peer HAVING SUM(state) - 1 > M;
	END
	$$
    LANGUAGE plpgsql;
-- BEGIN;
-- CALL get_peers_exit(30, 1);
-- FETCH ALL FROM "curs";
-- END;

---- Ex 23

CREATE OR REPLACE PROCEDURE get_last_come_peer(
	c refcursor = 'curs')
	AS $$
	BEGIN
		OPEN c FOR
			SELECT peer FROM timetracking
			WHERE state = 1 
			AND time = (SELECT MAX(time) 
						FROM timetracking 
						WHERE state = 1)
			AND date = current_date;
	END
	$$
	LANGUAGE plpgsql;
-- 	BEGIN;
-- 	CALL get_last_come_peer();
-- 	FETCH ALL FROM "curs";
-- 	END;

---- EX 24

CREATE OR REPLACE PROCEDURE get_peers_tomorrow_break_more(
	N int
	, c refcursor = 'curs')
	AS $$
	BEGIN
	OPEN c FOR
		SELECT peer
		FROM
			(SELECT peer, SUM(sum_time) AS interval_break
			FROM
				(SELECT peer, (-SUM(time)) AS sum_time
				FROM
					(SELECT peer, -(-time) AS time
					FROM timetracking
					WHERE date = current_date - 1 and state = 2
					UNION
					SELECT peer, -time AS time
					FROM timetracking
					WHERE date = current_date - 1 and state = 1) AS time_im_campus
				GROUP BY peer
				UNION
				SELECT peer, SUM(time) AS sum_time
				FROM
					(SELECT peer, (-(-(MAX(time)))) AS time
					FROM timetracking
					WHERE date = current_date - 1 and state = 2
					GROUP BY peer
					UNION
					SELECT peer, (-min(time)) AS time 
					FROM timetracking
					WHERE date = current_date - 1 and state = 1
					GROUP BY peer) AS time_im_campus_without_break
				GROUP BY peer) AS temp
			GROUP BY peer) AS all_inteval_break
		WHERE interval_break > make_interval(mins := N);
	END
	$$
	LANGUAGE plpgsql;
-- BEGIN;
-- CALL get_peers_tomorrow_break_more(0, 'curs');
-- FETCH ALL FROM "curs";
-- END;

---- EX 25 

CREATE OR REPLACE FUNCTION get_percent_early_enters(month1 VARCHAR) RETURNS
TABLE (EarlyEntries int) AS $$
	BEGIN
		RETURN QUERY (
			WITH all_enter_early_12_ AS (
			SELECT COUNT(state) AS s
				FROM timetracking t
				INNER JOIN peers p ON to_char(p.birthday, 'mm') = to_char(t.date, 'mm')
				WHERE state = 1
					AND to_char(date, 'mm') = month1
					AND time <= '12:00:00')
			SELECT (all_enter_early_12_.s::numeric / all_enter.s * 100)::int AS EarlyEntries
				FROM all_enter_early_12_,
				(SELECT COUNT(state) AS s
				FROM timetracking t
				WHERE state = 1) AS all_enter);
	RETURN;
	END
	$$
	LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE get_percent_early_enters_res(
	c refcursor = 'curs')
	AS $$
	BEGIN
		OPEN c FOR
			SELECT month
			, (SELECT EarlyEntries 
				FROM get_percent_early_enters(calendar.num)
				)
			FROM
				(SELECT to_char(gs::date, 'mm') AS num
						, to_char(gs::date, 'month') AS month
				FROM generate_series('2023-01-01', '2023-12-01', interval '1 month') AS gs
				)AS calendar;
	END
	$$
	LANGUAGE plpgsql;
-- 	BEGIN;
-- 	CALL get_percent_early_enters_res();
-- 	FETCH ALL FROM "curs";
-- 	END;


