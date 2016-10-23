book1 = LOAD 'hdfs://cshadoop1/user/ssb151030/Assignment6/Input/all-bible' using PigStorage(' ') as (line:charArray);

B = FOREACH book1 GENERATE FLATTEN(TOKENIZE((chararray)$0)) AS word;
Y = FOREACH B GENERATE LOWER(word) As word;
Z = FOREACH Y GENERATE TRIM(word) as word;
C = FILTER Z BY word MATCHES '[a-z]+';
D = FILTER C BY (SIZE($0)==5);
E = GROUP D BY word;
book1_count = FOREACH E GENERATE group, COUNT(D) as count2:long;

book2 = LOAD 'hdfs://cshadoop1/user/ssb151030/Assignment6/Input/shakespere' using PigStorage(' ') as (line:charArray);
B = FOREACH book2 GENERATE FLATTEN(TOKENIZE((chararray)$0)) AS word;
Y = FOREACH B GENERATE LOWER(word) As word;
Z = FOREACH Y GENERATE TRIM(word) as word;
C = FILTER Z BY word MATCHES '[a-z]+';
D = FILTER C BY (SIZE(word)==5);
E = GROUP D BY word;
book2_count = FOREACH E GENERATE group, COUNT(D) as count2:long;

result = JOIN book1_count BY $0, book2_count BY $0;
result1 = FOREACH result generate $0,$1,$2,$3,($1+$3);

order_desc = ORDER result1 BY $4 DESC;

common_words = LIMIT order_desc 10;

result1= FOREACH common_words GENERATE $0,$4;

STORE result1 INTO 'Part2_Pig_result' USING PigStorage(' '); 