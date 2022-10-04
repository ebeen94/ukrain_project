----------- upload clean tweet with vader scores 

CREATE TABLE IF NOT EXISTS tweets(date varchar(100),
                                   time varchar(100),
                                  username varchar(100),
                                  language varchar(100),
                                  clean_tweet text,
                                  compound decimal,
                                  neg decimal,
                                  neu decimal,
                                  pos decimal)

COPY tweets
FROM '/Users/charlottechoi/projects/ukraine_project-main/data/eng_clean_tweets_vader_scores.csv'
DELIMITER ',' CSV HEADER;
                            
SELECT *
from tweets
limit 10;

ALTER TABLE tweets
ALTER date type date
using date::date;

---------------tweet keywords
SELECT unnest(string_to_array(clean_tweet, ' ')), count(*)
from tweets 
group by 1
order by 2 desc

---------------tweet keywords throughout time
SELECT date, unnest(string_to_array(clean_tweet, ' ')), count(*)
from tweets 
group by 1,2
having count(*) >= 10
order by 1 asc, 3 desc


---------------tweet sentiments
select round(100* avg(compound),2) as compound, 
        round(100* avg(neg),2) as neg, 
        round(100* avg(neu),2) as neu, 
        round(100* avg(pos),2) as pos
from tweets

---------------tweet sentiments throughout time
select date, round(100* avg(compound),2) as compound, 
        round(100* avg(neg),2) as neg, 
        round(100* avg(neu),2) as neu, 
        round(100* avg(pos),2) as pos
from tweets
group by 1
order by 1 asc

















