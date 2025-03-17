import pandas as pd
import numpy as np

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here

    # Cleaning
    df = df.dropna()
    df = df.drop_duplicates()


    # Create datetime dimension table
    dim_datetime = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].reset_index(drop=True)
    # Pick Up
    dim_datetime['tpep_pickup_datetime'] = dim_datetime['tpep_pickup_datetime']
    dim_datetime['pick_hour'] = dim_datetime['tpep_pickup_datetime'].dt.hour
    dim_datetime['pick_day'] = dim_datetime['tpep_pickup_datetime'].dt.day
    dim_datetime['pick_month'] = dim_datetime['tpep_pickup_datetime'].dt.month
    dim_datetime['pick_year'] = dim_datetime['tpep_pickup_datetime'].dt.year
    dim_datetime['pick_weekday'] = dim_datetime['tpep_pickup_datetime'].dt.weekday
    # Drop off
    dim_datetime['tpep_dropoff_datetime'] = dim_datetime['tpep_dropoff_datetime']
    dim_datetime['drop_hour'] = dim_datetime['tpep_dropoff_datetime'].dt.hour
    dim_datetime['drop_day'] = dim_datetime['tpep_dropoff_datetime'].dt.day
    dim_datetime['drop_month'] = dim_datetime['tpep_dropoff_datetime'].dt.month
    dim_datetime['drop_year'] = dim_datetime['tpep_dropoff_datetime'].dt.year
    dim_datetime['drop_weekday'] = dim_datetime['tpep_dropoff_datetime'].dt.weekday
    # Create datatimeID
    dim_datetime['datetime_id'] = dim_datetime.index
    # Sort datetime_dim
    dim_datetime = dim_datetime[
    ['datetime_id', 
    'tpep_pickup_datetime', 'pick_hour', 'pick_day', 'pick_month', 'pick_year', 'pick_weekday',
    'tpep_dropoff_datetime', 'drop_hour', 'drop_day', 'drop_month', 'drop_year', 'drop_weekday']
    ]


    # Rate_code dimension table
    rate_code_type = {
        1:"Standard rate",
        2:"JFK",
        3:"Newark",
        4:"Nassau or Westchester",
        5:"Negotiated fare",
        6:"Group ride"
    }
    dim_rate_code = df[['RatecodeID']].reset_index(drop=True)
    dim_rate_code['rate_code_id'] = dim_rate_code.index
    dim_rate_code['rate_code_name'] = dim_rate_code['RatecodeID'].map(rate_code_type)
    dim_rate_code = dim_rate_code[['rate_code_id','RatecodeID','rate_code_name']]
    dim_rate_code = dim_rate_code.fillna('Unknown')


    # Add zone info from TLC
    zone_df = pd.read_csv('https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv')
    zone_df.fillna('Unknown', inplace=True)

    # Pickup Dim
    dim_pickup = df[['PULocationID']].reset_index(drop=True)
    dim_pickup['pickup_id'] = dim_pickup.index
    dim_pickup = dim_pickup[['pickup_id','PULocationID']]
    # merge on 'ID'
    dim_pickup = pd.merge(dim_pickup, zone_df, left_on='PULocationID', right_on='LocationID', how='inner')
    dim_pickup.drop(columns=["LocationID"], inplace=True)
    dim_pickup = dim_pickup.rename(columns={
        "PULocationID":"pu_location_id",
        "Borough":"pu_borough",
        "Zone":"pu_zone",
        "service_zone":"pu_service_zone"})
    # Dropoff Dim
    dim_dropoff = df[['DOLocationID']].reset_index(drop=True)
    dim_dropoff['dropoff_id'] = dim_dropoff.index
    dim_dropoff = dim_dropoff[['dropoff_id','DOLocationID']]
    # merge on 'ID'
    dim_dropoff = pd.merge(dim_dropoff, zone_df, left_on='DOLocationID', right_on='LocationID', how='inner')
    dim_dropoff.drop(columns=["LocationID"], inplace=True)
    dim_dropoff = dim_dropoff.rename(columns={
        "DOLocationID":"do_location_id",
        "Borough":"do_borough",
        "Zone":"do_zone",
        "service_zone":"do_service_zone"})



    # Dim_payment_type
    payment_type_name = {
        1:"Credit card",
        2:"Cash",
        3:"No charge",
        4:"Dispute",
        5:"Unknown",
        6:"Voided trip"
    }
    dim_payment_type = df[['payment_type']].reset_index(drop=True)
    dim_payment_type['payment_type_id'] = dim_payment_type.index
    dim_payment_type['payment_type_name'] = dim_payment_type['payment_type'].map(payment_type_name)
    dim_payment_type = dim_payment_type[['payment_type_id','payment_type','payment_type_name']]


    # Fact table
    fact_table = df.merge(dim_datetime, left_on='trip_id', right_on='datetime_id') \
                .merge(dim_payment_type, left_on='trip_id', right_on='payment_type_id') \
                .merge(dim_rate_code, left_on='trip_id', right_on='rate_code_id') \
                .merge(dim_pickup, left_on='trip_id', right_on='pickup_id') \
                .merge(dim_dropoff, left_on='trip_id', right_on='dropoff_id')\
                [['trip_id','VendorID', 'datetime_id', 'rate_code_id',
                'store_and_fwd_flag', 'pickup_id', 'dropoff_id', 'payment_type_id', 'passenger_count',
                'trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee']]

    return dim_datetime, dim_payment_type, dim_rate_code, dim_pickup, dim_dropoff, fact_table
    # {"dim_datetime":dim_datetime, 
    # "dim_payment_type":dim_payment_type, 
    # "dim_rate_code":dim_rate_code,
    # "dim_pickup":dim_pickup,
    # "dim_dropoff":dim_dropoff,
    # "fact_table":fact_table} 
    


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

# result = transform(df)
# print(type(result))