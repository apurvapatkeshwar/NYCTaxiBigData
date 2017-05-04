/* Load the Taxi Data */

input_file = LOAD 'project2' USING PigStorage(',') AS (VendorID,lpep_pickup_datetime,Lpep_dropoff_datetime,Store_and_fwd_flag,RateCodeID,Pickup_longitude,Pickup_latitude,Dropoff_longitude,Dropoff_latitude,Passenger_count,Trip_distance,Fare_amount,Extra,MTA_tax,Tip_amount,Tolls_amount,Ehail_fee,improvement_surcharge,Total_amount,Payment_type,Trip_type);

/* Ignore the header line */
Ranked = RANK input_file;

NoHeader = FILTER Ranked BY (rank_input_file > 2);

Ordered = ORDER NoHeader BY rank_input_file;

New_input_file = FOREACH Ordered GENERATE VendorID,lpep_pickup_datetime,Lpep_dropoff_datetime,Store_and_fwd_flag,RateCodeID,Pickup_longitude,Pickup_latitude,Dropoff_longitude,Dropoff_latitude,Passenger_count,Trip_distance,Fare_amount,Extra,MTA_tax,Tip_amount,Tolls_amount,Ehail_fee,improvement_surcharge,Total_amount,Payment_type,Trip_type;

/* Make Sure that Vendor ID is only 1 or 2 */
new_file_1 = FILTER New_input_file BY (VendorID == 1) OR (VendorID == 2); 

/* Make Sure that Rate Code ID is only 1 or 2 */
new_file_1 = FILTER new_file_1 BY (RateCodeID >= 1) and (RateCodeID <= 2); 

/* Remove the bad enteries in the data */
new_file_1 = FILTER new_file_1 BY (Trip_distance >= 0) and (Fare_amount >= 0) and (Extra >= 0) and (MTA_tax >= 0) and (Tip_amount >= 0) and (Tolls_amount >= 0) and (Total_amount >= 0);

/* Payment Type must be between 1 to 6 */
new_file_1 = FILTER new_file_1 BY (Payment_type >= 1) and (Payment_type <= 6);

/* The Trip Type must be 1 or 2 */
new_file_1 = FILTER new_file_1 BY (Trip_type == 1) OR (Trip_type == 2);

/* The passenger count must be more than 1 */
new_file_1 = FILTER new_file_1 BY (Passenger_count >= 1);

store new_file_1 into 'output1' using PigStorage(',');






