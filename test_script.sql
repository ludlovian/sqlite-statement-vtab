.mode list
.echo on
.nullvalue NULL

SELECT 10, 'Load';
.load ./statement_vtab.so

SELECT 20, 'Create';
create virtual table vt1 using statement((
  select  :num + 1  as a,
          :str || 'x' as b
  union all
  select  :num + 2,
          :str || 'y'
));
select 21, name from sqlite_schema where name='vt1';

SELECT 30, 'Simple select';
SELECT 31, a, b from vt1 where num=7 and str='y';
SELECT 32, a, b from vt1 where num=7 and a > 8;
SELECT 33, a, b from vt1 limit 1;

SELECT 40, 'Cache on';
SELECT 41, statement_enable_cache(null);
SELECT 42, statement_enable_cache(1);
SELECT 43, * from vt1(7, 'y') as aa, vt1(17, 'z') as bb where aa.a=8 and bb.a=19;
SELECT 44, * from vt1(7, 'y') as aa, vt1(17, 'z') as bb where aa.a=8 and bb.a=19;
SELECT 45, statement_enable_cache(0);
