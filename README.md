# Simple API Request Script
This process will extract data from a URL and store its results in a SQLite database. 

## How to Run
### Option 1: Running with Poetry + Pyenv
This option is best for simple local runs. 
1. Switch pyenv to 3.12: `pyenv local 3.12`
1. Switch to Poetry shell: `poetry shell`
1. Install dependencies: `poetry install`
1. Run the script: `poetry run python src/main.py`

### Option 2: Running in a Docker Container
This route is recommended for production-grade deployments.
1. Build the Docker Image: `docker build --pull --rm -f "Dockerfile" -t getdatafromapi:latest "."`
1. Launch a container: `docker run -i -t getdatafromapi /bin/bash`
1. Run the script: `python src/main.py`

### Option 3: Create a Virtual Environment with virtualenv
This option is similar to option 1 but using native Python virtual enviornment functionality. 
1. Create a venv: `python3 -m venv path/to/venv`
1. Activate the venv: `source path/to/venv/bin/activate`
1. Install dependencies into the venv: `python3 -m pip install -r requirements.txt`
1. Run the script: `python3 src/main.py`
