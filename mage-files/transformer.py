import pandas as pd

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
    #drop duplicates and add the primary key
    df.drop_duplicates().reset_index(drop=True, inplace=True)
    df['trip_id'] = df.index + 1

    #convert datetime fields to proper format
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    #create datetime dimension table
    dim_datetime = df[['tpep_pickup_datetime', 'tpep_dropoff_datetime']].reset_index(drop=True)
    dim_datetime['pickup_hour'] = dim_datetime['tpep_pickup_datetime'].dt.hour
    dim_datetime['pickup_day'] = dim_datetime['tpep_pickup_datetime'].dt.day
    dim_datetime['pickup_month'] = dim_datetime['tpep_pickup_datetime'].dt.month
    dim_datetime['pickup_year'] = dim_datetime['tpep_pickup_datetime'].dt.year
    dim_datetime['pickup_weekday'] = dim_datetime['tpep_pickup_datetime'].dt.weekday
    dim_datetime['dropoff_hour'] = dim_datetime['tpep_dropoff_datetime'].dt.hour
    dim_datetime['dropoff_day'] = dim_datetime['tpep_dropoff_datetime'].dt.day
    dim_datetime['dropoff_month'] = dim_datetime['tpep_dropoff_datetime'].dt.month
    dim_datetime['dropoff_year'] = dim_datetime['tpep_dropoff_datetime'].dt.year
    dim_datetime['dropoff_weekday'] = dim_datetime['tpep_dropoff_datetime'].dt.weekday
    dim_datetime['datetime_id'] = dim_datetime.index + 1
    dim_datetime = dim_datetime[['datetime_id', 'tpep_pickup_datetime', 'pickup_hour', 'pickup_day', 'pickup_month', 'pickup_year'
                                ,'pickup_weekday', 'tpep_dropoff_datetime', 'dropoff_hour', 'dropoff_day', 'dropoff_month'
                                ,'dropoff_year', 'dropoff_weekday']]

    #create rate code dimension table
    rate_code_labels = {
        1: 'Standard rate',
        2: 'JFK',
        3: 'Neward',
        4: 'Nassau/Westchester',
        5: 'Negotiated fare',
        6: 'Group ride'
    }
    dim_rate_code = pd.DataFrame(rate_code_labels.items(), columns=['rate_code', 'rate_code_name'])
    #create the primary key from the index
    dim_rate_code['rate_code_id'] = dim_rate_code.index + 1
    #reorder the columns
    dim_rate_code = dim_rate_code[['rate_code_id', 'rate_code', 'rate_code_name']]

    #create payment type dimension table
    payment_type_lables = {
        1: 'Credit Card',
        2: 'Cash',
        3: 'No charge',
        4: 'Dispute',
        5: 'Unknown',
        6: 'Voided trip'
    }
    dim_payment_type = pd.DataFrame(payment_type_lables.items(), columns=['payment_type_code', 'payment_type_name'])
    dim_payment_type['payment_type_id'] = dim_payment_type.index + 1
    dim_payment_type = dim_payment_type[['payment_type_id', 'payment_type_code', 'payment_type_name']]

    #create location dimension table
    dim_location = df[['pickup_longitude','pickup_latitude','dropoff_longitude','dropoff_latitude']].reset_index(drop=True)
    dim_location['location_id'] = dim_location.index + 1
    dim_location = dim_location[['location_id','pickup_longitude','pickup_latitude','dropoff_longitude','dropoff_latitude']]

    #create fact table, starting by createing a copy of the original dataframe
    fact_table = df.copy()
    fact_table = fact_table.merge(dim_datetime, left_on='trip_id', right_on='datetime_id', how='left')
    fact_table = fact_table.merge(dim_location, left_on='trip_id', right_on='location_id', how='left')
    fact_table = fact_table.merge(dim_rate_code, left_on='RatecodeID', right_on='rate_code_id', how='left')
    fact_table = fact_table.merge(dim_payment_type, left_on='payment_type', right_on='payment_type_id', how='left')
    fct_trip = fact_table[['trip_id','VendorID','datetime_id','rate_code_id','payment_type_id','location_id','passenger_count'
                        ,'trip_distance','store_and_fwd_flag','fare_amount','extra','mta_tax','tip_amount','tolls_amount'
                        ,'improvement_surcharge','total_amount']]
    fct_trip.rename(columns={'VendorID': 'vendor_id', 'store_and_fwd_flag': 'store_fwd_flag'}, inplace=True)

    return {"fct_trip":fct_trip.to_dict(orient="dict"),
            "dim_datetime":dim_datetime.to_dict(orient="dict"),
            "dim_location":dim_location.to_dict(orient="dict"),
            "dim_rate_code":dim_rate_code.to_dict(orient="dict"),
            "dim_payment_type":dim_payment_type.to_dict(orient="dict")
    }


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
