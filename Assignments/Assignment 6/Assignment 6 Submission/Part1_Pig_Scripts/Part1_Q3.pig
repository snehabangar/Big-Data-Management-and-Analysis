review= LOAD 'hdfs://cshadoop1/yelpdatafall/review/review.csv' using PigStorage('^') as (reviewId,userId,businessId,stars);

R = FOREACH review generate $2,($1,$3);

business = LOAD 'hdfs://cshadoop1/yelpdatafall/business/business.csv' using PigStorage('^') as (businessId,fullAddress,categories);

B =FILTER business BY (fullAddress matches '.*Stanford.*');

B1 = FOREACH B GENERATE $0;

result = JOIN R BY $0, B1 BY $0;

user_star = DISTINCT result;

user_star = FOREACH user_star GENERATE $1;

STORE user_star INTO 'Q3_result' USING PigStorage(' '); 
 