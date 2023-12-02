CREATE DATABASE stockAnalysis;

USE stockAnalysis;

CREATE TABLE stockentriesdaily
(
  index_        MEDIUMINT,
  openStat		double,	
  highStat		double,
  lowStat		double,
  closeStat		double,
  timeStamp_    BIGINT,
  datetime_    	VARCHAR(20),
  PRIMARY KEY   (index_)	
);