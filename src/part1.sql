CREATE DATABASE info;

DROP TABLE IF EXISTS Peers CASCADE;
CREATE TABLE Peers (
    Nickname VARCHAR NOT NULL PRIMARY KEY ,
    Birthday date NOT NULL
);

DROP TABLE IF EXISTS Tasks CASCADE;
CREATE TABLE Tasks (
    Title VARCHAR NOT NULL PRIMARY KEY
    , ParentTask VARCHAR DEFAULT NULL REFERENCES  Tasks(Title)
    , MaxXP BIGINT NOT NULL
);

DROP TYPE IF EXISTS Check_status CASCADE;
CREATE TYPE Check_status AS ENUM ('Start', 'Success', 'Failure');

DROP TABLE IF EXISTS Checks CASCADE;
CREATE TABLE Checks (
    ID BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY
    , Peer VARCHAR NOT NULL REFERENCES Peers(Nickname)
    , Task VARCHAR NOT NULL REFERENCES Tasks(Title)
    , Date date NOT NULL
);

DROP TABLE IF EXISTS P2P CASCADE;
CREATE TABLE P2P (
    ID BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY
    , Check_ID BIGINT NOT NULL REFERENCES Checks(ID)
    , Checking_Peer VARCHAR NOT NULL REFERENCES Peers(Nickname)
    , State Check_status NOT NULL
    , Time TIME WITHOUT TIME ZONE
);

DROP TABLE IF EXISTS Verter CASCADE;
CREATE TABLE Verter (
  ID BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY
  , CheckID BIGINT NOT NULL REFERENCES Checks(ID)
  , State bool
  , Time TIME WITHOUT TIME ZONE NOT NULL
);

DROP TABLE IF EXISTS TransferredPoints CASCADE;
CREATE TABLE TransferredPoints (
    ID BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY
    , CheckingPeer VARCHAR NOT NULL REFERENCES Peers(Nickname)
    , CheckedPeer VARCHAR NOT NULL REFERENCES Peers(Nickname)
    , PointsAmount BIGINT NOT NULL
    , UNIQUE (CheckingPeer, CheckedPeer)
);

DROP TABLE IF EXISTS Friends CASCADE;
CREATE TABLE  Friends(
    ID BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY
    , Peer1 VARCHAR NOT NULL REFERENCES Peers(Nickname)
    , Peer2 VARCHAR NOT NULL REFERENCES Peers(Nickname)
);

DROP TABLE IF EXISTS Recommendations CASCADE;
CREATE TABLE Recommendations (
    ID BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY
    , Peer VARCHAR NOT NULL REFERENCES Peers(Nickname)
    , RecommendedPeer VARCHAR NOT NULL REFERENCES  Peers(Nickname)
);

DROP TABLE IF EXISTS TimeTracking CASCADE;
CREATE TABLE TimeTracking (
    ID BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY
    , Peer VARCHAR NOT NULL REFERENCES Peers(Nickname)
    , Date date NOT NULL
    , Time time WITHOUT TIME ZONE NOT NULL
    , State int CHECK (State = 1 OR State = 2)
);

DROP TABLE IF EXISTS XP;
CREATE TABLE XP (
    ID BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY
    , CheckID BIGINT NOT NULL REFERENCES Checks(ID)
    , XPAmount BIGINT
);
insert into Peers(nickname, birthday) values
    ('cresswec', '2003-04-13'),
    ('mbulwer', '1970-07-23'),
    ('werewolf', '1998-01-12'),
    ('ajhin', '1996-01-03'),
    ('barnards', '1909-01-01');

insert into Friends(Peer1, Peer2) values
    ('cresswec', 'werewolf'),
	('barnards', 'werewolf'),
	('ajhin', 'cresswec'),
	('mbulwer', 'ajhin');

insert into Recommendations(Peer, RecommendedPeer) values
    ('werewolf', 'barnards'),
    ('ajhin', 'cresswec'),
    ('cresswec','mbulwer'),
    ('mbulwer', 'ajhin'),
    ('barnards', 'ajhin'),
	('ajhin', 'werewolf'),
	('ajhin', 'cresswec'),
	('ajhin', 'barnards'),
	('werewolf', 'ajhin');

insert into Tasks(title, parenttask, maxxp) values
    ('C2_SimpleBashUtils', null, 350),
    ('C3_s21_string+', 'C2_SimpleBashUtils', 750),
    ('C4_s21_math', 'C2_SimpleBashUtils', 300),
    ('C5_s21_decimal', 'C2_SimpleBashUtils', 350),
    ('C6_s21_matrix', 'C5_s21_decimal', 200),
    ('C7_SmartCalc_v1.0', 'C6_s21_matrix', 500),
    ('D01_Linux', 'C2_SimpleBashUtils', 300),
    ('D02_linuxNetwork','D01_Linux', 250);

insert into Checks(peer, task, date) values
    ('ajhin', 'C2_SimpleBashUtils', '2022-01-01'),
    ('mbulwer', 'C2_SimpleBashUtils', '2022-01-01'),
    ('werewolf', 'C2_SimpleBashUtils', '2022-01-01'),
    ('werewolf', 'D01_Linux', '2022-01-01'),
    ('barnards', 'C2_SimpleBashUtils', '2022-01-01');

insert into P2P(check_id,Checking_Peer, State, Time) values
    (1, 'mbulwer', 'Start', '12:00'),
    (1, 'mbulwer', 'Failure', '12:45'),
    (2, 'ajhin', 'Start', '15:00'),
    (2, 'ajhin', 'Success', '15:45'),
    (3, 'werewolf', 'Start', '16:00'),
    (3, 'werewolf', 'Success', '16:45'),
    (4, 'barnards', 'Start', '20:00'),
    (4, 'barnards', 'Success', '20:45'),
    (5, 'werewolf', 'Start', '23:00'),
    (5, 'werewolf', 'Success', '00:45');


insert into TransferredPoints(CheckingPeer, CheckedPeer, PointsAmount)
	SELECT p1.nickname as Checking_Peer
	       , p2.nickname as CheckedPeer
	       , 0 as PointsAmount
	FROM  peers p2
	CROSS JOIN peers p1
	WHERE p2.nickname <> p1.nickname;

insert into Verter(checkid, state, time) values
    (1, true, '12:50'),
    (2, true, '15:50'),
    (3, true, '16:50'),
    (4, true, '20:50'),
    (5, true, '00:50');

insert into XP(checkid, xpamount) values
    (1, 350),
    (2, 292),
    (3, 328),
    (4, 323),
    (5, 322);

insert into TimeTracking(peer, date, time, state) values
    ('werewolf', '2023-01-15', '10:00', 1),
    ('werewolf', '2023-01-15', '23:59', 2),
    ('cresswec', '2023-02-01', '19:00', 1),
    ('cresswec', '2023-02-01', '19:10', 2),
    ('cresswec', '2023-02-01', '19:50', 1),
    ('cresswec', '2023-02-01', '23:01', 2),
    ('mbulwer', '2023-02-01', '12:00', 1),
    ('mbulwer', '2023-02-01', '15:00', 2),
    ('mbulwer', '2023-02-01', '19:15', 1),
    ('mbulwer', '2023-02-01', '21:15', 2),
    ('mbulwer', '2023-07-15', '12:00', 1),
    ('mbulwer', '2023-07-15', '21:15', 2);

CREATE OR REPLACE  PROCEDURE import_from_csv(
tableName text
, fileName text
, delim text)
LANGUAGE plpgsql
AS $$
    BEGIN
        EXECUTE format('COPY %s TO %L DELIMITER %s CSV HEADER', tableName, fileName, delim);
    END
$$;

CREATE OR REPLACE PROCEDURE export_to_cvs(
tableName text
, fileName text
, delim text)  AS $$
    BEGIN
        EXECUTE (format('COPY %s FROM %L DELIMITER %s CSV HEADER', tableName, fileName, delim));
    END
$$
LANGUAGE plpgsql;

