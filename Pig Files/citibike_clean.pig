input_file = LOAD '201601-citibike-tripdata.csv' USING PigStorage(',') AS (trip_duration, start_time, stop_time, start_station_id, start_station_name, start_station_latitude, start_station_longitude, end_station_id, end_station_name, end_station_latitude, end_station_longitude, bikeid, usertype, birth_year, gender);

Ranked = RANK input_file;

NoHeader = FILTER Ranked BY (rank_input_file > 1);

Ordered = ORDER NoHeader BY rank_input_file;

New_input_file = FOREACH Ordered GENERATE trip_duration, start_time, stop_time, start_station_id, start_station_name, start_station_latitude, start_station_longitude, end_station_id, end_station_name, end_station_latitude, end_station_longitude, bikeid, usertype, birth_year, gender;


filtered_data = Filter New_input_file BY (end_station_id > 0) AND (start_station_id > 0) AND ( gender >= 0) AND (gender < 3) AND (birth_year != '' ) AND (bikeid > 0) AND (trip_duration <= 21600);

STORE filtered_data INTO 'Filtered_Citi_Bike' USING PigStorage(',');