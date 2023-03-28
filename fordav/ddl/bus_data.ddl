CREATE TABLE IF NOT EXISTS fordav.bus_data 
( 
	vehicle_id varchar(255) encode Text255 not NULL, 
	scene_id varchar(255) encode Text255 not NULL, 
	data_ts BIGINT not NULL sortkey,
	angular_velocity_x FLOAT4 not NULL, 
	angular_velocity_y FLOAT4 not NULL, 
	angular_velocity_z FLOAT4 not NULL, 
	linear_acceleration_x FLOAT4 not NULL, 
	linear_acceleration_y FLOAT4 not NULL, 
	linear_acceleration_z FLOAT4 not NULL, 
	latitude FLOAT4 not NULL, 
	longitude FLOAT4 not NULL, 
	altitude FLOAT4 not NULL,
	primary key(vehicle_id, scene_id, data_ts), 
	FOREIGN KEY(vehicle_id) references fordav.vehicle(vehicleid) 
) DISTSTYLE AUTO;