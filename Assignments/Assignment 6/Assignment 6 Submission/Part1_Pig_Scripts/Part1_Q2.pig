review= LOAD 'hdfs://cshadoop1/yelpdatafall/review/review.csv' using PigStorage('^') as (reviewId,userId,businessId,stars);

R = FOREACH review generate $1,$3;

reviewGrp = GROUP R BY ($0);
review_avg =  FOREACH reviewGrp GENERATE group, AVG(R.$1) AS avgRating;

user= LOAD 'hdfs://cshadoop1/yelpdatafall/user/user.csv' using PigStorage('^') as (userId,name,url);

match_user = FILTER user BY (name == '$USERNAME');

result = JOIN match_user BY $0, review_avg BY $0;

user_review_avg = FOREACH result GENERATE $0,$4;

STORE user_review_avg INTO 'Q2_result' USING PigStorage(' '); 
