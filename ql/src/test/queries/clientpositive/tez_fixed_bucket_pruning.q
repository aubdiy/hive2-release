-- fixed bucket pruning on
set hive.tez.bucket.pruning=true;

CREATE TABLE `test_table`( `col_1` int,`col_2` string,`col_3` string)
        CLUSTERED BY (col_1) INTO 4 BUCKETS;

insert into test_table values(1, 'one', 'ONE'), (2, 'two', 'TWO'), (3,'three','THREE'),(4,'four','FOUR');

select * from test_table;

explain extended select col_1, col_2, col_3 from test_table where col_1 <> 2 order by col_2;
select col_1, col_2, col_3 from test_table where col_1 <> 2 order by col_2;

explain extended select col_1, col_2, col_3 from test_table where col_1 = 2 order by col_2;
select col_1, col_2, col_3 from test_table where col_1 = 2 order by col_2;

drop table `test_table`;