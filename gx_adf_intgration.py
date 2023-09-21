import os
import subprocess
import sys

USER_TOKEN = "***"
GX_CLOUD_ORGANIZATION_ID = "***"
COLUMN_NAME = "vendor_id" # set column name you want to test here

# Define the URL to the 'get-pip.py' script
get_pip_script_url = 'get-pip.py'

# Run the 'python -m ensurepip' command to ensure that pip is installed
subprocess.run(['python', '-m', 'ensurepip'])

# Run the 'python -m pip install --upgrade get-pip.py' command to upgrade pip
# and install the 'get-pip.py' script, capturing the output and treating it as text
subprocess.run(['python', '-m', 'pip', 'install', '--upgrade', get_pip_script_url], capture_output=True, text=True)
 
# Specify the path to the Python interpreter
python_path = '/usr/bin/python'

# Command for installing GX 
command = [python_path, '-m', 'pip', 'install', '-U', 'great_expectations']

# Run the command
result = subprocess.run(command, capture_output=True, text=True)

# Check the return code
if result.returncode == 0:
    print("Command ran successfully")
else:
    print(f"Command failed with return code {result.returncode}")
    print("Standard Error:")
print(result.stderr)


# Append a path to the sys.path list, allowing Python to find modules in this directory
# Please find the right PATH through result.stderr
sys.path.append('/mnt/batch/tasks/workitems/*************/wd/.local/lib/python3.9/site-packages')
import great_expectations as gx


# Get the version of great_expectations
ge_version = gx.__version__

# The integration has been tested on version 0.17.18
print(f"Great Expectations version: {ge_version}")


os.environ["GX_CLOUD_ACCESS_TOKEN"] = USER_TOKEN
os.environ["GX_CLOUD_ORGANIZATION_ID"] = GX_CLOUD_ORGANIZATION_ID



context = gx.get_context()

#### Define Datasource #### 
datasource_name = f"BCI_ADF_datasource"

if datasource_name in [datasource['name'] for datasource in context.list_datasources()]:
    datasource = context.get_datasource(datasource_name)
else:
    datasource = context.sources.add_pandas(datasource_name)

#### Define Assets #### 
asset_name = f"ADF_asset"
# to use sample data uncomment next line
path_to_data = "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"

try:
    asset = datasource.get_asset(asset_name)
except Exception as e:
    asset = datasource.add_csv_asset(asset_name, filepath_or_buffer=path_to_data)

# Build batch request
batch_request = asset.build_batch_request()


#### Define Suites #### 
expectation_suite_name = f"BCI_ADF_suite"

if expectation_suite_name in context.list_expectation_suite_names(): 

    expectation_suite = context.get_expectation_suite(expectation_suite_name)    
else:
    # Get an existing Expectation Suite
    expectation_suite = context.add_expectation_suite(expectation_suite_name=expectation_suite_name)

    # Look up all expectations types here - https://greatexpectations.io/expectations/
    expectation_configuration = gx.core.ExpectationConfiguration(**{
    "expectation_type": "expect_column_to_exist",
    "kwargs": {
        "column": COLUMN_NAME,
    },
    "meta":{},
    })

    expectation_suite.add_expectation(
        expectation_configuration=expectation_configuration
    )

    # Save the Expectation Suite
    context.save_expectation_suite(expectation_suite=expectation_suite)

#### Run Checkpoints #### 

checkpoint_name = f'BCI_ADF_checkpoint' # name your checkpoint here

if checkpoint_name in context.list_checkpoints():
    checkpoint = context.get_checkpoint(name = checkpoint_name)

    checkpoint.run()
else:

    checkpoint_config = {
    "name": checkpoint_name,
    "validations": [{
        "expectation_suite_name": expectation_suite_name,
        "expectation_suite_ge_cloud_id": expectation_suite.ge_cloud_id,
        "batch_request": {
            "datasource_name": datasource.name,
            "data_asset_name": asset.name,
        },
    }],
    "config_version": 1,
    "class_name": "Checkpoint"
    }

    checkpoint = context.add_or_update_checkpoint(**checkpoint_config)
    checkpoint.run()