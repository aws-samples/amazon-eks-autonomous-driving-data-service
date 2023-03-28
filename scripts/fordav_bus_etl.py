'''
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
'''

import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

from pyspark.sql.types import LongType
from pyspark.sql.functions import udf
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
args = getResolvedOptions(sys.argv, ['database', 's3_bucket', 's3_output_prefix'])

# set your bucket name below
database = args['database']
s3_bucket= args['s3_bucket']
s3_output_prefix = args['s3_output_prefix']

print(f"database: {database}")
print(f"s3_bucket: {s3_bucket}")
print(f"s3_output_prefix: {s3_output_prefix}")

idyf = glueContext.create_dynamic_frame.from_catalog(database=database, table_name='imu')
idyf.printSchema()

gdyf = glueContext.create_dynamic_frame.from_catalog(database=database, table_name='gps')
gdyf.printSchema()

def data_ts(stamp):
    return int(stamp.secs*10**6 + stamp.nsecs/1000)

data_ts_udf = udf(data_ts, LongType())

idf = idyf.toDF()
idf = idf.select(idf.vehicle_id.alias('vehicle_id'), 
                      idf.scene_id.alias('scene_id'), 
                      data_ts_udf(idf.header.stamp).alias('data_ts'),
                      idf.angular_velocity.x.alias('angular_velocity_x'),
                      idf.angular_velocity.y.alias('angular_velocity_y'),
                      idf.angular_velocity.z.alias('angular_velocity_z'),
                      idf.linear_acceleration.x.alias('linear_acceleration_x'),
                      idf.linear_acceleration.y.alias('linear_acceleration_y'),
                      idf.linear_acceleration.z.alias('linear_acceleration_z'))
idf.printSchema()

gdf = gdyf.toDF()
gdf = gdf.select(gdf.vehicle_id.alias('vehicle_id'), 
                      gdf.scene_id.alias('scene_id'), 
                      data_ts_udf(gdf.header.stamp).alias('data_ts'),
                      gdf.latitude.alias('latitude'),
                      gdf.longitude.alias('longitude'),
                      gdf.altitude.alias('altitude'))
gdf.printSchema()

bus_df = idf.join(gdf, (idf.vehicle_id == gdf.vehicle_id) & (idf.scene_id == gdf.scene_id) & (idf.data_ts == gdf.data_ts), "inner").select(
                          idf.vehicle_id.alias('vehicle_id'),
                          idf.scene_id.alias('scene_id'),
                          idf.data_ts.alias('data_ts'),
                          idf.angular_velocity_x, idf.angular_velocity_y, idf.angular_velocity_z,
                          idf.linear_acceleration_x, idf.linear_acceleration_y,idf.linear_acceleration_z,
                          gdf.latitude, gdf.longitude, gdf.altitude)

bus_df.printSchema()
bus_df.write.save(f"s3://{s3_bucket}/{s3_output_prefix}", format='csv', header=True)
