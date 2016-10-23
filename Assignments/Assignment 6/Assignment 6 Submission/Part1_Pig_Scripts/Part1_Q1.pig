review= LOAD 'hdfs://cshadoop1/user/ssb151030/Yelp/review.csv' using PigStorage('^') as (reviewId,userId,businessId,
stars);
R = FOREACH review generate $2,$3;
reviewGrp = GROUP R BY ($0);
review_avg =  FOREACH reviewGrp GENERATE group, AVG(R.$1) AS avgRating;
order_desc = ORDER review_avg BY $1 DESC;
top_business = LIMIT order_desc 10;
business = LOAD 'hdfs://cshadoop1/user/ssb151030/Yelp/business.csv' using PigStorage('^') as (businessId,
fullAddress, categories);
result = JOIN business BY $0, top_business BY $0;
temp = DISTINCT result;
business_review_rating = FOREACH temp GENERATE $0,($1,$2,$4);
STORE business_review_rating INTO 'Q1_result' USING PigStorage(' '); 
