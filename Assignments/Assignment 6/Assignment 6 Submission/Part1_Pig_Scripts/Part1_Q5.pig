review= LOAD 'hdfs://cshadoop1/yelpdatafall/review/review.csv' using PigStorage('^') as (reviewId,userId,businessId,stars);

R = FOREACH review generate $2,$0;
R1= GROUP R BY ($0);
business_count =  FOREACH R1 GENERATE group, COUNT($1) AS businessCount;

business = LOAD 'hdfs://cshadoop1/yelpdatafall/business/business.csv' using PigStorage('^') as (businessId,fullAddress,categories);
B = FOREACH business generate $0,$1;
B1 = FILTER B BY (fullAddress matches '.*, TX.*');
B1= DISTINCT B1;
result = JOIN business_count BY $0, B1 BY $0;

busiReview = FOREACH result generate $0,$1;
STORE busiReview INTO 'Q5_result' USING PigStorage(' '); 