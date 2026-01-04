import great_expectations as gx

def get_context():
    return gx.get_context(
        project_root_dir="/opt/airflow/great_expectations"
    )