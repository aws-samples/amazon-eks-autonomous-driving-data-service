CREATE TABLE IF NOT EXISTS a2d2.bus_data 
( 
	vehicle_id varchar(255) encode Text255 not NULL, 
	scene_id varchar(255) encode Text255 not NULL, 
	data_ts BIGINT not NULL sortkey, 
	acceleration_x FLOAT4 not NULL, 
	acceleration_y FLOAT4 not NULL, 
	acceleration_z FLOAT4 not NULL, 
	accelerator_pedal FLOAT4 not NULL, 
	accelerator_pedal_gradient_sign SMALLINT not NULL, 
	angular_velocity_omega_x FLOAT4 not NULL, 
	angular_velocity_omega_y FLOAT4 not NULL, 
	angular_velocity_omega_z FLOAT4 not NULL, 
	brake_pressure FLOAT4 not NULL, 
	distance_pulse_front_left FLOAT4 not NULL, 
	distance_pulse_front_right FLOAT4 not NULL, 
	distance_pulse_rear_left FLOAT4 not NULL, 
	distance_pulse_rear_right FLOAT4 not NULL, 
	latitude_degree FLOAT4 not NULL, 
	latitude_direction SMALLINT not NULL, 
	longitude_degree FLOAT4 not NULL, 
	longitude_direction SMALLINT not NULL, 
	pitch_angle FLOAT4 not NULL, 
	roll_angle FLOAT4 not NULL, 
	steering_angle_calculated FLOAT4 not NULL, 
	steering_angle_calculated_sign SMALLINT not NULL, 
	vehicle_speed FLOAT4 not NULL, 
	primary key(vehicle_id, scene_id, data_ts), 
	FOREIGN KEY(vehicle_id) references a2d2.vehicle(vehicleid) 
) DISTSTYLE AUTO;