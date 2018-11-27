--code by yizheng ying 
--z5141180
--28/04/2018
--Q1:

drop type if exists RoomRecord cascade;
create type RoomRecord as (valid_room_number integer, bigger_room_number integer);

create or replace function Q1(course_id integer)
    returns RoomRecord
as $$
--... SQL statements, possibly using other views/functions defined by you ...
DECLARE cou1 integer;
DECLARE cou2 integer;
DECLARE cap1 integer;
DECLARE cap2 integer;
DECLARE che integer;
BEGIN 
      select count(*) into che from course_enrolments where course=$1;
      if (che=0)
      then raise exception 'INVALID COURSEID';
      ELSE
      select count(*) into cou1 from course_enrolments where course=$1;
      select count(*) into cou2 from course_enrolment_waitlist where course=$1;
      select count(*) into cap1 from rooms where capacity>=cou1;
      select count(*) into cap2 from rooms where capacity>=(cou1+cou2);
      END if;
      return (cap1,cap2);
      
END;
$$ language plpgsql;

--Q2V1:
create or replace view Q2V1(staff,id,term,code,name,uoc,mark)
as 
select staff,courses.id,substring(to_char(year,'9999')from 4 for 2)||lower(term) as term,code,subjects.name,uoc,mark from courses
join course_staff on (courses.id=course_staff.course)
join semesters on (semesters.id=courses.semester)
join subjects on (courses.subject=subjects.id)
join course_enrolments on (course_enrolments.course=courses.id)
where mark is not null
;
--course,mark
create or replace view cm(course,mark)
as
select course,mark from course_enrolments where mark is not null
;
--median()
create or replace function median(anyarray)
       returns float8 strict
as $$
DECLARE cnt integer;
BEGIN
cnt:=(select count(*) from unnest($1) val
where val is not null);
return (select avg(val)::float8
       from (select val from unnest($1) val
       where val is not null
       order by 1
       limit 2- mod(cnt,2)
       offset ceil(cnt/2.0)-1
       )as tmp
       );
end;

$$ language plpgsql 
;
drop aggregate if exists median(anyelement);
create aggregate median(anyelement)(
SFUNC=array_append,
STYPE=anyarray,
FINALFUNC=median,
INITCOND='{}'
);
--Q2_COURSE:
--create or replace function Q2_course(staff_id,integer)
--returns integer
--as $$
--select course from Q2V1 WHERE staff=$1;
--$$ language plpgsql;

--Q2_TERM:
create or replace function Q2_term(course integer,staffid integer)
returns char(4)
as $$
select term from Q2V1 where id=$1 and staff=$2;
$$ language sql;

--Q2_CODE:
create or replace function Q2_code(integer,integer)
returns char(8)
as $$
select code from Q2V1 where id=$1 and staff=$2;
$$ language sql;

--Q2_NAME:
create or replace function Q2_name(integer,integer)
returns text
as $$
select name from Q2V1 where id=$1 and staff=$2;
$$ language sql;

--Q2_UOC:
create or replace function Q2_uoc(integer,integer)
returns integer
as $$
declare uo integer;
begin 
select uoc into uo from Q2V1 where id=$1 and staff=$2;
if (uo=null)then
return null;
else
return uo;
end if;
end;
$$ language plpgsql;

--Q2_avg:
create or replace function Q2_avg(integer,integer)
returns integer
as $$
select cast(round(avg(mark))as integer) from Q2V1 where id=$1 and staff=$2; 
$$ language sql;
--Q2_highest:
create or replace function Q2_highest(integer,integer)
returns integer
as $$
select cast(round(max(mark))as integer) from Q2V1 where id=$1 and staff=$2; 
$$ language sql;
--Q2_median:
create or replace function Q2_median(integer,integer)
returns integer
as $$
select cast(round(median(mark))as integer) from Q2V1 where id=$1 and staff=$2; 
$$ language sql;
--Q2_total:
create or replace function Q2_total(integer,integer)
returns integer
as $$
declare total integer;
begin
select count(*) into total from Q2V1 where id=$1 and staff=$2;
if (total<>0)then 
return total;
end if;
end;
$$ language plpgsql;

--Q2:

drop type if exists TeachingRecord cascade;
create type TeachingRecord as (cid integer, term char(4), code char(8), name text, uoc integer, average_mark integer, highest_mark integer, median_mark integer, totalEnrols integer);

create or replace function Q2(staff_id integer)
	returns setof TeachingRecord
as $$
DECLARE cou integer;
declare r TeachingRecord;
BEGIN
 select count(*) into cou from course_staff where staff=$1;
      if (cou=0)
      then raise exception 'INVALID STAFFID';
ELSE
--... SQL statements, possibly using other views/functions defined by you ...
for r in 
select  id,
Q2_term(id,$1) as term,
Q2_code(id,$1) as code,
Q2_name(id,$1) as name,
Q2_uoc(id,$1) as uoc,
Q2_avg(id,$1),
Q2_highest(id,$1),
Q2_median(id,$1),
Q2_total(id,$1) from Q2V1
where staff=$1 group by id,term,code,name,uoc loop
return next r;
end loop;
END if;
return;
END;
$$ language plpgsql;

--Q3recursive
create or replace function Q3_rec(owner integer)
returns table(member integer)
as $$
with recursive all_org(member) as ( select member from orgunit_groups
where owner=$1 union select og.member from all_org ao,orgunit_groups og
where og.owner=ao.member)
select member from all_org union select $1
$$ language sql;
--Q3_orgunits,orgunit_group,subjects,courses
create or replace function osc(oid integer)
returns table(cid integer,oname text,code char(8),sname text,sem text)
as $$
select courses.id as cid,orgunits.name as oname,subjects.code,subjects.name as sname,semesters.name as sem from
Q3_rec($1)as a
--(select member from orgunit_groups where owner=$1 union select $1)as a
join orgunits on(orgunits.id=a.member)
join subjects on (subjects.offeredby=a.member)
join courses on (courses.subject=subjects.id)
join semesters on (semesters.id=courses.semester);
$$ language sql;
--Q3_uid,name,id
create or replace function idname(score integer,oid integer)
returns table(student integer,unswid integer,uname text)
as $$
select distinct student,unswid,name as uname from course_enrolments
join osc($2) a on (a.cid=course_enrolments.course)
join people on
(people.id=course_enrolments.student)
where mark>=$1;
$$ language sql;
--num_of_course
create or replace function num_course(coursenum integer, score integer,oid integer)
returns table(student integer,unswid integer,uname text)
as $$
select c.student,c.unswid,c.name from(select student,people.name,unswid
from
(select course_enrolments.student,count(course) from course_enrolments
join osc($3) a on (a.cid=course_enrolments.course)
group by course_enrolments.student having(count(course)>$1))as b
join people on (b.student=people.id))as c
join idname($2,$3) d on (d.unswid=c.unswid);
$$ language sql;
--final_table

create or replace function final_table(oid integer,coursenum integer,score integer)
returns table(unswid integer,uname text,code char(8),
sname text,sem text,oname text,mark integer,course integer)
as $$
select unswid,uname,code,sname,sem,oname,mark,course from 
num_course($2,$3,$1) as a join
course_enrolments on (course_enrolments.student=a.student)
join osc($1) b on (b.cid=course_enrolments.course)
order by mark desc nulls last,course asc;
$$ language sql;

--Q3:

drop type if exists CourseRecord cascade;
create type CourseRecord as (unswid integer, student_name text, course_records text);

drop type if exists CourseRecord1 cascade;
create type CourseRecord1 as (unswid integer, student_name text, course_records text,mark integer, course integer);
--Q3_tem

create or replace function Q3_tem(org_id integer, num_courses integer, min_score integer)
returns setof CourseRecord1
as $$
DECLARE cr CourseRecord1;
	r record;
	x integer;
	y text;
	countnum integer;
BEGIN
	for x,y in (select unswid,uname from num_course($2,$3,$1))
	loop
	cr.unswid:=x;
	cr.student_name:=y;
	conutnum:=0;
	for r in 
	    (select code,sname,sem,oname,mark,course 
	    from final_table($1,$2,$3) where 
	    unswid=x and uname=y)
	    loop
	    cr.course_records :=r.code||', '||r.sname||', '||
	    r.sem||', '||r.oname||', '||r.mark||E'\n';
	    cr.mark:=r.mark;
	    cr.course=r.course;
          countnum:=countnum+1;
	    return next cr;
	    exit when countnum=5;
	    end loop;

	END loop;
return;
END;
$$ language plpgsql;


create or replace function Q3(org_id integer, num_courses integer, min_score integer)
  returns setof CourseRecord
as $$
declare cou integer;
BEGIN

select count(id) into cou from orgunits where id=$1;
if (cou=0)then 
raise exception 'INVALID ORGID';
ELSE
return query
(
select unswid,student_name,string_agg(course_records,'' order by mark desc 
nulls last,course asc)
from Q3_tem($1,$2,$3) group by unswid,student_name
);
END if;
END;
--... SQL statements, possibly using other views/functions defined by you ...
$$ language plpgsql;
