--Problem 2:

--1) select region_stud,city,count(*) from studentschools group by region_stud,city;

--2) select lvl_school, city, count(*) from escuelasPR group by city,lvl_school;

--3) select count(*) from studentschools where sex_stud = 'F' and city = 'Ponce' and lvl_school = --'Superior';

--4) select count(*),region_school,district_stud,city from studentschools where sex_stud = 'M' group by --region_school,district_stud, city;


--CODE HERE

create table estudiantesPR (region_stud STRING, district_stud STRING, id_school_stud INT, school_name_stud STRING, lvl_school_stud STRING, sex_stud CHAR(1), id_student INT) row format delimited
fields terminated by ',';

create table escuelasPR (region_school STRING, district_school STRING, city STRING, id_school INT, school_name STRING, lvl_school STRING, college INT) row format delimited
fields terminated by ',';

load data local inpath "/home/alejandra/Documents/5toA~o_1erSemestre/BigDataAnalytics_ICOM5995/exam1/exam1-sp17-bigdata-desc-master/hive/studentsPR.csv" into table studentsPR;

load data local inpath "/home/alejandra/Documents/5toA~o_1erSemestre/BigDataAnalytics_ICOM5995/exam1/exam1-sp17-bigdata-desc-master/hive/escuelasPR.csv" into table schoolsPR;

Create table studentschools as
select *
from estudiantespr as S, escuelaspr as E
Where  S.id_school_stud = E.id_school;

select region_stud,city,count(*) 
from studentschools
group by region_stud,city;


select lvl_school, city, count(*) 
from escuelasPR 
group by city, lvl_school;

select count(*) 
from studentschools
where sex_stud = 'F' and city = 'Ponce' and lvl_school = 'Superior';

select region_stud,district_school,city,count(*) 
from studentschools
where sex_stud =  'M'
group by region_stud,district_school,city;



