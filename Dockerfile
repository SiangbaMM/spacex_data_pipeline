FROM quay.io/astronomer/astro-runtime:12.7.0

RUN python3 -m venv env_dbt
RUN python3 -m venv env_singer


# Copy and install dependencies for each environment
# Using uv for faster package installation
COPY requirements_*.txt  /tmp/

# dbt environment
RUN . env_dbt/bin/activate && \
    pip install -r /tmp/requirements_dbt.txt && \
    deactivate

# Singer environment
RUN . env_singer/bin/activate && \
    pip install -r /tmp/requirements_singer.txt && \
    pip install -e singer_tap/ && \
    deactivate
