CREATE DATABASE  IF NOT EXISTS `tweets`;

use `tweets`;

drop table if exists TWEET;

create table TWEET (
tweet_id   INT,
user_id  INT,
tweet_text  VARCHAR(140),
tweet_ts  DATETIME,
primary key (tweet_id)
);


drop table if exists FOLLOWS_;

create table FOLLOWS_ (
user_id int,
follows_id int
);


LOAD DATA local INFILE '/Users/liqi/Desktop/DS4300/follows_sample.csv' 
INTO TABLE FOLLOWS_ 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

select * from FOLLOWS_;


Insert into TWEET Values
(1,1,'Go NEU #NEU',now(6)),
(2,1,"evevrervwr",now(6)),
(3,1,"kwjhciumn jhwgcywe",now(6)),
(4,2,"ljdhcuew ishdc",now(6)),
(5,2, "wdcwrjlch",now(6)),
(6,5,"wuegciyw ywvc",now(6));

Select * from TWEET;


Select tweet_text from TWEET where user_id in 
(select follows_id from FOLLOWS_ where user_id = 1);