input_file = LOAD 'yellow_tripdata_2016-01.csv' USING PigStorage(',') AS (VendorID,lpep_pickup_datetime,Lpep_dropoff_datetime,Passenger_count,Trip_distance,Pickup_longitude,Pickup_latitude,RateCodeID,Store_and_fwd_flag,Dropoff_longitude,Dropoff_latitude,Payment_type,Fare_amount,Extra,MTA_tax,Tip_amount,Tolls_amount,improvement_surcharge,Total_amount);

Ranked = RANK input_file;

NoHeader = FILTER Ranked BY (rank_input_file > 2);

Ordered = ORDER NoHeader BY rank_input_file;

New_input_file = FOREACH Ordered GENERATE VendorID,lpep_pickup_datetime,Lpep_dropoff_datetime,Passenger_count,Trip_distance,Pickup_longitude,Pickup_latitude,RateCodeID,Store_and_fwd_flag,Dropoff_longitude,Dropoff_latitude,Payment_type,Fare_amount,Extra,MTA_tax,Tip_amount,Tolls_amount,improvement_surcharge,Total_amount;

new_file_1 = FILTER New_input_file BY (VendorID == 1) OR (VendorID == 2); 

new_file_1 = FILTER new_file_1 BY (RateCodeID >= 1) and (RateCodeID <= 2); 

new_file_1 = FILTER new_file_1 BY (Trip_distance >= 0) and (Fare_amount >= 0) and (Extra >= 0) and (MTA_tax >= 0) and (Tip_amount >= 0) and (Tolls_amount >= 0) and (Total_amount >= 0);

new_file_1 = FILTER new_file_1 BY (Payment_type >= 1) and (Payment_type <= 6);

new_file_1 = FILTER new_file_1 BY (Passenger_count >= 1);

STORE new_file_1 INTO 'cleaned_yellow_data' USING PigStorage(',');