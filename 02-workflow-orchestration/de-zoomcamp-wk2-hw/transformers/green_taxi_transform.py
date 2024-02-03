if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

snake_colname = {
        "VendorID": "vendor_id",
        "RatecodeID": "rate_code_id",
        "PULocationID": "pu_location_id",
        "DOLocationID": "do_location_id",
        }


@transformer
def transform(data, *args, **kwargs):
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
    data = data[(data.passenger_count > 0) & (data.trip_distance > 0)]
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    print(f'Unique VendorID: {data.VendorID.unique()}')
    data = data.rename(columns=snake_colname)
    print(f'Number of columns to rename: {len(snake_colname)}')
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert 'vendor_id' in list(output.columns), 'vendor_id not in dataframe columns'
    assert all(output.passenger_count.unique()) > 0, 'Passenger count contans 0'
    assert all(output.trip_distance.unique()) > 0, 'Trip distance contans 0'
    