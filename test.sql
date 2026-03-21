.mode list
.bail on
.nullvalue NULL

.testcase 10-load
.load ./statement_vtab.so
select 'ok';
.check ok

.testcase 20-create
create virtual table vt1 using statement((
  select  :num + 1  as a,
          :str || 'x' as b
));
select name from sqlite_schema where name='vt1';
.check <<END
vt1
END

.testcase 30-select-all-params
select a,b from vt1 where num=7 and str='y';
.check <<END
8|yx
END

.testcase 40-select-some-params
select a,b from vt1 where num=7;
.check <<END
8|NULL
END

.testcase 50-select-no-params
select a,b from vt1;
.check <<END
NULL|NULL
END
