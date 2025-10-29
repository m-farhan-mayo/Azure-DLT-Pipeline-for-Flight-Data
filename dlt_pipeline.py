import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


#1 Booking Data
@dlt.table(
    name = 'stage_booking'
)

def stage_booking():
    df = spark.readStream.format('delta').load('/Volumes/workspace/bronze/bronze_vol/booking/data')
    return df


@dlt.view(
    name = 'trans_bookings'
)
def trans_bookings():
    df = spark.readStream.table('stage_booking')
    df = df.withColumn('amount', col('amount').cast(DoubleType()))\
    .withColumn('modified_date', current_timestamp())\
    .withColumn('booking_date', to_date(col('booking_date')))\
    .drop('_rescued_data')

    return df



rules = {
    'rule1':  'booking_id IS NOT NULL',
    'rule2':  'passenger_id IS NOT NULL'
}

@dlt.table(
    name = 'silver_booking'
)
@dlt.expect_all_or_drop(rules)
def silver_booking():
    df = spark.readStream.table('trans_bookings')
    return df  

###########################################################################################################################################

#2. Flight Data

@dlt.view(
    name = 'trans_flight'
)
def trans_flight():
    df = spark.readStream.format('delta').load('/Volumes/workspace/bronze/bronze_vol/flight/data/')

    df = df.withColumn('flight_date', col('flight_date').cast('date'))\
    .withColumn('modify_date', current_timestamp())\
    .drop('_rescued_data')
    

    return df


dlt.create_streaming_table('silver_flight')

dlt.create_auto_cdc_flow(
    target = "silver_flight",
    source = "trans_flight",
    keys = ["flight_id"],
    sequence_by=col("modify_date"),
    stored_as_scd_type = 1
)

###########################################################################################################################################

#3. Passangers Data

@dlt.view(
    name = 'trans_passangers'
)
def trans_flight():
    df =  spark.readStream.format('delta').load('/Volumes/workspace/bronze/bronze_vol/customer/data/')

    df = df.withColumn('modify_date', current_timestamp())\
            .drop('_rescued_data')

    return df

dlt.create_streaming_table('silver_passangers')

dlt.create_auto_cdc_flow(
    target = "silver_passangers",
    source = "trans_passangers",
    keys = ["passenger_id"],
    sequence_by=col("modify_date"),
    stored_as_scd_type = 1
)

###########################################################################################################################################

#Airport Data

@dlt.view(
    name = 'trans_airport'
)
def trans_flight():
    df =  spark.readStream.format('delta').load('/Volumes/workspace/bronze/bronze_vol/airport/data/')

    df = df.withColumn('modify_date', current_timestamp())\
            .drop('_rescued_data')

    return df

dlt.create_streaming_table('silver_airport')

dlt.create_auto_cdc_flow(
    target = "silver_airport",
    source = "trans_airport",
    keys = ["airport_id"],
    sequence_by=col("modify_date"),
    stored_as_scd_type = 1
)


###########################################################################################################################################

#Silver Business View

@dlt.table(
    name = 'silver_business'
)
def silver_business():
    df = dlt.readStream('silver_booking')\
            .join(dlt.readStream('silver_flight'), ['flight_id'])\
            .join(dlt.readStream('silver_passangers'), ['passenger_id'])\
            .join(dlt.readStream('silver_airport'), ['airport_id'])\
            .drop('modify_date')

    return df




