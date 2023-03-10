CREATE OR REPLACE PROCEDURE add_P2P(
    checkedPeer text
    ,checkingPeer text
    , taskName text
    , stateP2P text
    , long time without time zone
) AS $$
    BEGIN
    IF stateP2P = 'Start' THEN
        INSERT INTO checks (peer, task, Date)
            VALUES(checkedPeer, taskName, current_date);
        INSERT INTO p2p(check_id, checking_peer, state, time)
            VALUES (
                (SELECT max(id) FROM checks)
                , checkingPeer
                , CAST(stateP2P AS check_status)
                , long
               );
    ELSE
        INSERT INTO p2p(check_id, checking_peer, state, time)
        VALUES ((SELECT DISTINCT check_id 
                FROM (SELECT check_id, checking_peer
                     FROM p2p GROUP BY check_id, checking_peer
                     HAVING COUNT(state) = 1) as t
                INNER JOIN checks ON checks.peer = checkedPeer
                       AND task = taskName
                       AND t.checking_peer = checkingPeer)
                , checkingPeer
                , CAST(stateP2P AS check_status)
                , long);
    END IF;
    END
    $$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE add_Verter(
    checkedPeer text
	, taskname text
	, stateVerter bool
	, long time
) AS
    $$
    DECLARE
        count bigint = (SELECT c.id
                        FROM checks c
                        JOIN p2p p ON c.id = p.check_id
                        WHERE c.task = taskname
                        AND c.peer = checkedPeer
                        AND p.state = 'Success'
                        ORDER BY time DESC
                        LIMIT 1)::bigint;
    BEGIN
        IF (count IS NOT NULL) THEN
            IF (NOT EXISTS(SELECT checkid
                           FROM verter
                           WHERE checkid = count
                           AND state = stateVerter)) THEN
                INSERT INTO verter (checkid, state, time)
                VALUES (count, stateVerter, long);
            END IF;
        END IF;
    END
    $$
    LANGUAGE plpgsql;
	
-- 	CALL add_Verter('mbulwer'::text
-- 					, 'D01_Linux'::text
-- 					, true
-- 				   , current_time(0)::time );


CREATE OR REPLACE FUNCTION add_row_in_TransferredPoints() RETURNS TRIGGER AS $tg_Trans_Point$
    BEGIN
    IF (TG_OP = 'INSERT') THEN
        IF NEW.state = 'Start' THEN
            UPDATE TransferredPoints SET pointsamount = pointsamount + 1
            WHERE checkingpeer = NEW.checking_peer
            AND checkedpeer = (SELECT peer
                                FROM checks
                                WHERE NEW.check_id = checks.id);
            RETURN NEW;
        END IF;
    END IF;
    RETURN NULL;
    END;
    $tg_Trans_Point$ LANGUAGE plpgsql;

-- SELECT peer FROM checks c INNER JOIN p2p p ON p.check_id = c.id;

CREATE OR REPLACE TRIGGER add_start_in_P2P
    AFTER INSERT ON p2p
    FOR EACH ROW EXECUTE PROCEDURE add_row_in_TransferredPoints();


CREATE OR REPLACE FUNCTION add_row_in_XP() RETURNS TRIGGER AS
$tg_Trans_Point$
DECLARE
    max bigint = (SELECT maxxp
                    FROM tasks
                    JOIN checks on tasks.title = checks.task
                    WHERE checks.id = new.checkid)::bigint;
    check_exists bool = (EXISTS(SELECT state
                            FROM p2p
                            JOIN checks ON checks.id = check_id
                            WHERE checks.id = new.checkid
                            AND state = 'Success'));
BEGIN
    IF (TG_OP = 'INSERT') THEN
        IF (NEW.xpamount > 0 AND NEW.xpamount <= max AND check_exists) THEN
            RETURN NEW;
        END IF;
    END IF;
    RETURN OLD;
END;
$tg_Trans_Point$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER correct_xp
    BEFORE INSERT
    ON xp
    FOR EACH ROW
EXECUTE PROCEDURE add_row_in_XP();

-- insert into XP(checkid, xpamount) values
--     (6, 350)
-- 	,(9, 500);


-- call add_P2P('mbulwer'
--     , 'ajhin'
--     , format('%s', 'C4_s21_math')
--     , 'Start'
--     , localtime);

-- call add_P2P('ajhin'
--     , 'mbulwer'
--     , format('%s', 'C4_s21_math')
--     , 'Start'
--     , localtime);

-- call add_P2P('ajhin'
--     , 'mbulwer'
--     , format('%s', 'C5_s21_decimal')
--     , 'Start'
--     , localtime);

-- call add_P2P('mbulwer'
--     , 'barnards'
--     , format('%s', 'D01_Linux')
--     , 'Start'
--     , localtime);

-- call add_P2P('mbulwer'
--     , 'barnards'
--     , format('%s', 'D01_Linux')
--     , 'Success'
--     , localtime);

-- call add_P2P('barnards'
--     , 'werewolf'
--     , format('%s', 'D01_Linux')
--     , 'Start'
--     , localtime);

-- call add_P2P('barnards'
--     , 'werewolf'
--     , format('%s', 'D01_Linux')
--     , 'Success'
--     , localtime);

-- call add_P2P('barnards'
--     , 'werewolf'
--     , format('%s', 'D02_linuxNetwork')
--     , 'Start'
--     , localtime);

-- call add_P2P('barnards'
--     , 'werewolf'
--     , format('%s', 'D02_linuxNetwork')
--     , 'Success'
--     , localtime);
-- call add_P2P('barnards'
--     , 'werewolf'
--     , format('%s', 'D02_linuxNetwork')
--     , 'Start'
--     , localtime);
-- call add_P2P('barnards'
--     , 'werewolf'
--     , format('%s', 'D02_linuxNetwork')
--     , 'Failure'
--     , localtime);
--     , localtime);

