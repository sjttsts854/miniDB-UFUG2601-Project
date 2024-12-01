CREATE DATABASE db_university;

USE DATABASE db_university;

CREATE TABLE student (
ID INTEGER,
Name TEXT,
GPA FLOAT,
Major TEXT
);

INSERT INTO student VALUES (1000, 'Jay Chou', 3.0, 'Microelectronics');

UPDATE student
SET GPA = 1.5, Major = 'Music'
WHERE ID = 1000;


SELECT * FROM student;
