CREATE KEYSPACE vrcard WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE vrcard;

CREATE TABLE entrance_by_poi (
	poi_name Text,
	entrance_timestamp Timestamp,
	card_serial Text,
	activation_date Date,
	profile_type Text,
	device_id int,
	PRIMARY KEY ((poi_name), entrance_timestamp, card_serial))
WITH CLUSTERING ORDER BY (entrance_timestamp ASC, card_serial ASC);

COPY entrance_by_poi (entrance_timestamp,poi_name,device_id,card_serial,activation_date,profile_type)
FROM 'dati_2014 sistemati.csv'
WITH HEADER = true;

COPY entrance_by_poi (entrance_timestamp,poi_name,device_id,card_serial,activation_date,profile_type)
FROM 'dati_2015 sistemati.csv'
WITH HEADER = true;

COPY entrance_by_poi (entrance_timestamp,poi_name,device_id,card_serial,activation_date,profile_type)
FROM 'dati_2016 sistemati.csv'
WITH HEADER = true;

COPY entrance_by_poi (entrance_timestamp,poi_name,device_id,card_serial,activation_date,profile_type)
FROM 'dati_2017 sistemati.csv'
WITH HEADER = true;

COPY entrance_by_poi (entrance_timestamp,poi_name,device_id,card_serial,activation_date,profile_type)
FROM 'dati_2018 sistemati.csv'
WITH HEADER = true;

COPY entrance_by_poi (entrance_timestamp,poi_name,device_id,card_serial,activation_date,profile_type)
FROM 'dati_2019 sistemati.csv'
WITH HEADER = true;

COPY entrance_by_poi (entrance_timestamp,poi_name,device_id,card_serial,activation_date,profile_type)
FROM 'dati_2020 sistemati.csv'
WITH HEADER = true;

CREATE TABLE entrance_by_card (
	card_serial Text,
	entrance_timestamp Timestamp,
	poi_name Text,
	activation_date Date,
	profile_type Text,
	device_id int,
	PRIMARY KEY ((card_serial), entrance_timestamp, poi_name))
WITH CLUSTERING ORDER BY (entrance_timestamp ASC, poi_name ASC);

COPY entrance_by_card (entrance_timestamp,poi_name,device_id,card_serial,activation_date,profile_type)
FROM 'dati_2014 sistemati.csv'
WITH HEADER = true;

COPY entrance_by_card (entrance_timestamp,poi_name,device_id,card_serial,activation_date,profile_type)
FROM 'dati_2015 sistemati.csv'
WITH HEADER = true;

COPY entrance_by_card (entrance_timestamp,poi_name,device_id,card_serial,activation_date,profile_type)
FROM 'dati_2016 sistemati.csv'
WITH HEADER = true;

COPY entrance_by_card (entrance_timestamp,poi_name,device_id,card_serial,activation_date,profile_type)
FROM 'dati_2017 sistemati.csv'
WITH HEADER = true;

COPY entrance_by_card (entrance_timestamp,poi_name,device_id,card_serial,activation_date,profile_type)
FROM 'dati_2018 sistemati.csv'
WITH HEADER = true;

COPY entrance_by_card (entrance_timestamp,poi_name,device_id,card_serial,activation_date,profile_type)
FROM 'dati_2019 sistemati.csv'
WITH HEADER = true;

COPY entrance_by_card (entrance_timestamp,poi_name,device_id,card_serial,activation_date,profile_type)
FROM 'dati_2020 sistemati.csv'
WITH HEADER = true;




