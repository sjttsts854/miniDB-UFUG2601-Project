CREATE DATABASE db_university;
USE DATABASE db_university;
CREATE TABLE student (
ID INTEGER,
Name TEXT,
GPA FLOAT
);
INSERT INTO student VALUES (1000, 'Jay Chou', 3.0);
INSERT INTO student VALUES (1001, 'Taylor Swift', 3.2);
INSERT INTO student VALUES (1002, 'Bob Dylan', 3.5);
SELECT ID, Name, GPA FROM student;