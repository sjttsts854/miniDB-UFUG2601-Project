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
SET Major = 'Data Science'
WHERE Name = 'Jay Chou';


UPDATE student
SET GPA = (GPA -0.1) * 0.95;

SELECT * FROM student;