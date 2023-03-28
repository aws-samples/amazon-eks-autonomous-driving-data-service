create table
fordav.drive_data
(
vehicle_id varchar(255) encode Text255 not NULL,
scene_id varchar(255) encode Text255 not NULL,
sensor_id varchar(255) encode Text255 not NULL,
data_ts BIGINT not NULL sortkey,
s3_bucket VARCHAR(255) encode lzo NOT NULL,
s3_key varchar(255) encode lzo NOT NULL,
primary key(vehicle_id, scene_id, sensor_id, data_ts),
FOREIGN KEY(vehicle_id) references fordav.vehicle(vehicleid),
FOREIGN KEY(sensor_id) references fordav.sensor(sensorid)
)
DISTSTYLE AUTO
;