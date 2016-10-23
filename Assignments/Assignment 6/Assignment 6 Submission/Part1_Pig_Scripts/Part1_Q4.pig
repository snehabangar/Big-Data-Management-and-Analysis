review= LOAD 'hdfs://cshadoop1/yelpdatafall/review/review.csv' using PigStorage('^') as (reviewId:charArray,userId:charArray,businessId:charArray,
stars:float);
R = FOREACH review generate $1,$0;
R1= GROUP R BY ($0);
review_sum =  FOREACH R1 GENERATE group, COUNT($1) AS reviewCount;
user_order_desc = ORDER review_sum BY $1 DESC;
user_order_desc = LIMIT user_order_desc 10;
user= LOAD 'hdfs://cshadoop1/yelpdatafall/user/user.csv' using PigStorage('^') as (userId,name,url);
U = FOREACH user generate $0,$1;
U1 = DISTINCT U;
result = JOIN user_order_desc BY $0, U1 BY $0;
temp = DISTINCT result;
top_user= FOREACH temp GENERATE $0,$3;
STORE top_user INTO 'Q4_result' USING PigStorage(' '); 
