FROM astrocrpublic.azurecr.io/runtime:3.3-2

# The runtime image's ONBUILD hooks copy this repo to /usr/local/airflow and
# pip-install requirements.txt (astronomer-cosmos). Install the trendscope
# package itself (EL + digest code; yfinance/anthropic/duckdb via pyproject).
RUN pip install --no-cache-dir -e /usr/local/airflow

# dbt gets its own Python 3.12 virtualenv: dbt-core does not yet support the
# Python 3.14 this runtime image ships, and a dedicated venv also keeps dbt's
# dependency tree fully decoupled from Airflow's. Cosmos invokes the venv's
# dbt executable via subprocess.
ENV TRENDSCOPE_DBT_EXECUTABLE=/home/astro/dbt-venv/bin/dbt \
    DBT_PROFILES_DIR=/usr/local/airflow/dbt
RUN pip install --no-cache-dir uv && \
    uv venv /home/astro/dbt-venv --python 3.12 && \
    uv pip install --python /home/astro/dbt-venv/bin/python \
        "dbt-core>=1.9,<2.0" "dbt-duckdb>=1.9,<2.0"

# Container-side paths. The DuckDB file and digests live under include/,
# which `astro dev` mounts from the host — so data survives restarts and
# artifacts are inspectable at ./include/ on the host.
ENV TRENDSCOPE_SETTINGS_PATH=/usr/local/airflow/config/settings.yaml \
    TRENDSCOPE_PATHS__UNIVERSE_YAML=/usr/local/airflow/config/universe.yaml \
    TRENDSCOPE_PATHS__DUCKDB=/usr/local/airflow/include/data/trendscope.duckdb \
    TRENDSCOPE_PATHS__DIGESTS=/usr/local/airflow/include/digests \
    TRENDSCOPE_DUCKDB_PATH=/usr/local/airflow/include/data/trendscope.duckdb \
    TRENDSCOPE_DBT_DIR=/usr/local/airflow/dbt

# Pre-install dbt packages (dbt_utils) so runtime tasks never hit the network
# for package resolution.
RUN cd /usr/local/airflow/dbt && /home/astro/dbt-venv/bin/dbt deps
