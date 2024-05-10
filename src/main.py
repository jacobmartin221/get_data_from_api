from typing import Union, Optional
import logging
import time
import sqlite3

import requests

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def retry_request(
    num_retries: int = 5, 
    success_list: list[int] = [200], 
    starting_sleep_time: Optional[Union[int, float]] = 0.5,
    **kwargs
) -> requests.models.Response:
    """Retry API request with a pause that grows exponentially with each 
    request until attempts are exhausted. 
    
    Params:
        num_retries (int): Number of times to retry request. Default set 
            to `num_retires = 5`.
        success_list (list[int]): Codes to consider successful 
            responses. Default set to `success_list = [200]`.
        starting_sleep_time (Optional[Union[int, float]]): Sleep time 
            between request attempts. If 0, 0.0, or None, requests will 
            continue to be made until the attempts equals `num_retries`. 
            Default set to `starting_sleep_time = 0.5`.
        **kwargs: Arguments for `request.get()`.
    
    Returns: 
        response (requests.models.Response): API request response.
    """
    seconds = starting_sleep_time
    for _ in range(num_retries):
        try:
            response = requests.get(**kwargs)
            if response.status_code in success_list:
                return response
            time.sleep(seconds)
        except requests.exceptions.ConnectionError as e:
            raise e
        seconds *= 2
    
    return response


def get_data(
    endpoint: str = "posts", 
    retry_attempts: int = 5
) -> list[dict]:
    """Get data from an API endpoint. 
    
    Params: 
        endpoint (str): Endpoint of URL. Default set to 
            `endpoint = "posts"`.
        retry (bool): Boolean of whether to retry request attempt or 
            not. Default set to `retry = True`.

    Returns: 
        list[dict]
    """
    base_url: str = "https://my-json-server.typicode.com"
    username: str = "jacobmartin221"
    project: str = "get_data_from_api"
    url: str = f"{base_url}/{username}/{project}/{endpoint}"

    logger.info("Making request")
    request_args = {
        "url": url, 
        "timeout": 60
    }
    response: requests.models.Response = retry_request(
        num_retries=retry_attempts, **request_args
    )
    # Check for succesful status
    response.raise_for_status()
    
    logger.info("Successful response of data")
    return response.json()


def add_length(data: list[dict], key: str) -> list[dict]:
    """Add a new key to each dictionary containing the length of a given 
    key's value.
    
    Params: 
        data (list[dict]): Input data. 
        key (str): Dictionary key value to utilize for measuring its 
            value's length. 
    
    Returns: 
        data (list[dict]): Input data with a new key-value pair for each 
            dictionary in the list.
    """
    for i in data:
        i[f"{key}_length"] = len(i[key])


def filter_data(data: list[dict], key: str, min_length: int) -> list[dict]:
    """"Filter out data where the `key` value is not at least N 
    characters.
    
    Params: 
        data (list[dict]): Data to filter.
        key (str): Dictionary key to use for filtering. 
        min_length (int): Minimum length of `key`.

    Returns: 
        filtered_data (list[dict])
    """
    filtered_data = []
    for i in data:
        if i.get(key) >= min_length:
            filtered_data.append(i)

    return filtered_data


def connect_to_database(path: str) -> sqlite3.Connection:
    """Connect to SQLite database. 
    
    Params:
        path (str): Database path.

    Returns: 
        sqlite3.Connection
    """
    return sqlite3.connect(path)


def create_table_if_not_exists(cursor: sqlite3.Cursor, name: str) -> None:
    """Create a SQLite table if it does not yet exist.
    
    Params: 
        cursor (sqlite3.Cursor): Database connection cursor. 
        name (str): Table name. 
    """
    try:
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {name}
            (
                id INT PRIMARY KEY NOT NULL, 
                title VARCHAR NOT NULL, 
                body VARCHAR NULL, 
                title_length INT NOT NULL
            );"""
        )
    except sqlite3.Error as e:
        raise e
    

def upsert_data(
    data: list[dict],
    table_name: str, 
    merge_column: str = "id"
) -> None:
    """Upsert data into SQLite database. 
    
    Params:
        cursor (sqlite3.Cursor): Database connection cursor. 
        table_name (str): Table name to upsert into.
        merge_column (str): Column to use for upserting. Defaut set to 
            `merge_column = "id"`.
    """
    columns = data[0].keys()
    # columns_str: str = ", ".join(columns)
    placeholders: str = ", ".join("?" * len(columns))
    set_map: str = ", ".join(
        [
            f"{column}=excluded.{column}" for column in columns 
            if column != merge_column
        ]
    )
    values: list[tuple] = [tuple(item.values()) for item in data]
    try:
        logger.info("Connecting to database")
        conn: sqlite3.Connection = connect_to_database(
            path="./request_results.db"
        )
        cursor: sqlite3.Cursor = conn.cursor()
        logger.info(f"Creating table {table_name} if it does not exist yet")
        create_table_if_not_exists(cursor=cursor, name=table_name)

        logger.info(f"Upsert data to table {table_name}")
        upsert_sql: str = f"""
            INSERT INTO {table_name} VALUES ({placeholders})
            ON CONFLICT({merge_column}) DO UPDATE SET {set_map};
        """
        cursor.executemany(upsert_sql, values)

        logger.info("Closing connection to database")
        conn.commit()
        conn.close()

    except sqlite3.Error as e:
        raise e


def _drop_table(cursor: sqlite3.Cursor, table: str) -> None:
    try:
        logger.info(f"Dropping table {table}")
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        logger.info(f"Table {table} dropped")
    except sqlite3.Error as e:
        raise e


def main(endpoint: str = "posts") -> None:
    """Retrieve and filter data from an API endpoint with results logged 
    to a SQLite database.

    Params: 
        endpoint (str): Endpoint of URL to hit. Database table will be 
            created with the same name. Default set to 
            `endpoint = "posts"`.
    """
    # Get data from endpoint
    logger.info("Making request to the REST API")
    data: list[dict] = get_data(endpoint=endpoint)
    logger.info(f"Length of retrieved data: {len(data)}")

    # Add length field to check title length
    logger.info("Adding a new length key-value pair to track title's length")
    add_length(data=data, key="title")

    # Filter data
    min_length: int = 5
    logger.info(
        "Filtering out data where length of `title` is not at least "
        f"{min_length} characters long"
    )
    filtered_data: list[dict] = filter_data(
        data=data, key="title_length", min_length=min_length
    )
    logger.info(f"Length of filtered data: {len(filtered_data)}")

    # Upload data to database
    logger.info("Beginning process to upsert data to database")
    upsert_data(
        data=filtered_data, table_name=endpoint, merge_column="id"
    )
    logger.info("Process complete.")
    

if __name__ == "__main__":
    main()
