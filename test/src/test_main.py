import pytest

from src.main import (
    add_length, 
    filter_data, 
    retry_request,
)


@pytest.fixture
def mock_data() -> list[dict]:
    """Mock input data from API. This output would be achieved from the 
    `get_data()` function. 
    
    Returns: 
        list[dict]
    """
    return [
        {"id": 100, "title": "Post 100", "body": "Incoming body 1"},
        {"id": 101, "title": "Post 101", "body": "Incoming body 2"},
        {"id": 102, "title": "Post 102", "body": "Incoming body 3"}, 
        {"id": 103, "title": "Post", "body": "Incoming body 4"}
    ]


@pytest.fixture
def mock_request_args() -> dict:
    """Mock request arguments for `requests.get()`.
    
    Returns: 
        dict
    """
    return {
        "url": "https://my-json-server.typicode.com/jacobmartin221/get_data_from_api/posts", 
        "timeout": 10
    }


def test_add_length(mock_data, key: str = "title") -> None:
    """Test the functionality of adding a new key to the starting data.

    Params: 
        mock_data: Function/fixture to create mock data. 
        key (str): Dictionary key to use for adding length.
    """
    # Assert both the column existing and working as it should
    # data = list(mock_data)  # make a copy for safety
    add_length(data=mock_data, key=key)
    length_key = f"{key}_length"
    for i in mock_data:
        assert i[length_key] == len(i[key])


def test_filter_data(mock_data) -> None:
    """Test the `filter_data()` function for functionality.
    
    Params: 
        mock_data: Function/fixture to create mock data. 
    """
    add_length(data=mock_data, key="title")
    filtered_data = filter_data(
        data=mock_data, key="title_length", min_length=5
    )

    # Length should be one less
    assert len(filtered_data) == len(mock_data) - 1


def test_retry_request(mock_request_args) -> None:
    """Test the API via the `retry_request()` function."""
    response = retry_request(**mock_request_args)
    assert response.status_code == 200
