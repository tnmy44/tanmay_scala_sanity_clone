from airflow.decorators import task
d=10

def squared_numbers(number):
    print('hello')

    return (number * number) / d

separator=' '

def join_2_strings(d1, d2):
    return f"{d1}{separator}{d2}"

separator=' '

def delete_me(d1, d2):
    return f"{d1}{separator}{d2}"



def load_resource(dag_dir, dag_id, relative_filepath):
    import os
    import zipfile
    import tempfile
    res = ""
    zip_file_found = None

    for filename in os.listdir(dag_dir):
        if filename.startswith(dag_id) and filename.endswith('.zip'):
            zip_file_found = os.path.join(dag_dir, filename)
            break

    if not zip_file_found:
        print(f"Zip for dag: {dag_id} not found")

        return res

    with tempfile.TemporaryDirectory() as tmpdirname:

        with zipfile.ZipFile(zip_file_found, 'r') as zip_ref:
            zip_ref.extractall(tmpdirname)

        target_file_path = os.path.join(tmpdirname, dag_id.lower(), 'resources', relative_filepath)

        if os.path.exists(target_file_path):
            with open(target_file_path, 'r') as file:
                res = file.read()

        return res



def get_feed_config(file_contents, search_keys):

    for line in file_contents.splitlines():
        # Split the line by commas and strip to remove leading/trailing whitespaces
        fields = [field.strip() for field in line.split(',')]

        # Check if there are enough fields
        if len(fields) < 32:
            continue

        # Extract the first 5 fields as keys
        keys = fields[:5]

        # Check if the current keys match the search keys
        if keys == search_keys:
            # Extract and return the remaining 27 fields as values
            return tuple(fields[5:])

    # Return an empty tuple if the key is not found or if the value is empty
    return ()


db_pipeline_id_to_path_dict = {
    "pipelines/CanDoRandomThingsBuddy": "dbfs:/FileStore/prophecy/artifacts/staging/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/CanDoRandomThingsBuddy.jar", 
    "pipelines/PankajLoadMoreDebug": "dbfs:/FileStore/prophecy/artifacts/staging/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/PankajLoadMoreDebug.jar", 
    "pipelines/Livy_sc_airflow_OS": "dbfs:/FileStore/prophecy/artifacts/staging/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/Livy_sc_airflow_OS.jar", 
    "pipelines/IO_SCALA_BASIC": "dbfs:/FileStore/prophecy/artifacts/staging/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/SM_IO_SCALA_BASIC.jar", 
    "pipelines/EM_DISABLED_SCALA_BASIC": "dbfs:/FileStore/prophecy/artifacts/staging/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/SM_DISABLED_SCALA_BASIC.jar", 
    "pipelines/SCALA_DEP_MGMT_ALL": "dbfs:/FileStore/prophecy/artifacts/staging/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/REL_SC_PIP_DEP_MGMT_ALL.jar", 
    "pipelines/SCALA_BASIC": "dbfs:/FileStore/prophecy/artifacts/staging/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/SCALA_BASIC.jar", 
    "pipelines/DONOT_OPEN_SCALA": "dbfs:/FileStore/prophecy/artifacts/staging/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/REL_SC_PIP_DONOT_OPEN.jar"
}


def task_wrapper(task_id):

    def decorator(func):

        @task(task_id = task_id)
        def wrapper(*args, **context):
            ## running the actual method.
            return func(*args, **context).execute(context)

        return wrapper

    return decorator



def find_package_name_db(path: str):
    return db_pipeline_id_to_path_dict[path].split("/")[- 1].replace("-1.0-py3-none-any.whl", "")
