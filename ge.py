from pprint import pprint
from ruamel.yaml import YAML

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.validator.validator import Validator
import pandas as pd
import numpy as np

from great_expectations.data_context.types.resource_identifiers import ExpectationSuiteIdentifier
from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig, FilesystemStoreBackendDefaults
from great_expectations.data_context import BaseDataContext
from great_expectations.core.batch import RuntimeBatchRequest

def create_data_context():

    data_context_config = DataContextConfig(
        datasources={
            "pandas_datasource": DatasourceConfig(
                class_name="Datasource",
                data_connectors={
                    "default_runtime_data_connector_name": {
                        "class_name": "RuntimeDataConnector",
                        "module_name": "great_expectations.datasource.data_connector",
                        "batch_identifiers": {"default_identifier_name": "default_identifier"},
                    }
                },
                execution_engine={
                    "class_name": "PandasExecutionEngine",
                    "module_name": "great_expectations.execution_engine",
                },
                module_name="great_expectations.datasource",
            )
        },
        store_backend_defaults=FilesystemStoreBackendDefaults(root_directory="C:/Users/bbutler/OneDrive - SciQuus Oncology/Desktop/Python/TFT/data/"),
    )

    context = BaseDataContext(project_config=data_context_config)
    
    return context

context = create_data_context()

def create_expectation_suite():
    
    suite = context.create_expectation_suite(
        expectation_suite_name="test_suite", overwrite_existing=True
    )

    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_table_row_count_to_equal",
        kwargs={
            "value": 100
            },
        meta={
           "notes": {
           "format": "markdown",
           "content": "Some clever comment about this expectation. **Markdown** `Supported`"
            }
        }
    )
    
    suite.add_expectation(expectation_configuration=expectation_configuration)

    return suite


def run():
    
    context = create_data_context()
    suite = create_expectation_suite()
    context.save_expectation_suite(suite)

    df = pd.DataFrame(np.random.randint(0,100,size=(100, 4)), columns=list('ABCD'))

    runtime_batch_request = RuntimeBatchRequest(
        datasource_name="pandas_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="default_data_asset_name",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "default_identifier"}
    )

    validator = context.get_validator(
        batch_request=runtime_batch_request,
        expectation_suite=suite
    )
    
    context.save_expectation_suite(suite)
    
    pprint(context.get_available_data_asset_names())
    context.list_expectation_suite_names()

    checkpoint_config = """
    name: default_checkpoint_name
    config_version: 1
    class_name: SimpleCheckpoint
    validations:
        - batch_request:
            datasource_name: pandas_datasource
            data_connector_name: default_runtime_data_connector_name
            data_asset_name: default_data_asset_name
    expectation_suite_name: test_suite
    """

    yaml = YAML()
    context.test_yaml_config(yaml_config=checkpoint_config)
    context.add_checkpoint(**yaml.load(checkpoint_config))

    context.run_checkpoint(
        checkpoint_name="default_checkpoint_name",
        batch_request={
            "runtime_parameters": {
                "batch_data": df,
            },
            "batch_identifiers": {
                "default_identifier_name": "default_identifier",
            }
        },
        run_name="test_run"
    )

    context.build_data_docs()

    context.open_data_docs()

if __name__=="__main__":
    run()