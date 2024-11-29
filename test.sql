CREATE DATABASE test;

USE DATABASE test;

CREATE TABLE student (
ID INTEGER,
Name TEXT,
GPA FLOAT,
Major TEXT
);

INSERT INTO student VALUES (1000, 'Jay Chou', 3.0, 'Microelectronics');

INSERT INTO student VALUES (1001, 'Taylor Swift', 3.2, 'Data Science');

INSERT INTO student VALUES (1002, 'Bob Dylan', 3.5, 'Financial Technology');

INSERT INTO student VALUES (1003, 'David Green', 3.7, 'Civil Engineering');

INSERT INTO student VALUES (1004, 'Hatsune Miku', 3.3, 'Vocaloid');

INSERT INTO student VALUES (1005, 'litterzy', 2.0, 'Cakewalk Producer');

SELECT * FROM student;

SELECT ID, Name FROM student;

