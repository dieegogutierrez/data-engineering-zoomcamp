import inflection

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    print(f'Preprocessing: rows with zero passengers: {data["passenger_count"].isin([0]).sum()}')
    print(f'Preprocessing: rows with zero distance: {data["trip_distance"].isin([0]).sum()}')

    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    old_columns = data.columns.to_list()
    new_columns = [inflection.underscore(column) for column in data.columns]

    changed_columns_count = sum(1 for i in range(len(old_columns)) if old_columns[i] != new_columns[i])

    data.columns = new_columns

    print(f"The unique values in 'vendor_id' column are: {data['vendor_id'].unique().tolist()}")
    print(f"The number of columns that changed names is: {changed_columns_count}")
    
    return data[(data['passenger_count'] != 0) & (data['trip_distance'] != 0)]

@test
def test_output(output, *args):
    assert 'vendor_id' in output.columns, "There is no column named vendor_id"
    assert (output["passenger_count"] > 0).all(), 'There are rides with zero passengers or less'
    assert (output["trip_distance"] > 0).all(), 'There are rides with zero distance or less'