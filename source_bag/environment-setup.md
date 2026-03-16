python3 -m venv .venv

pip3 install dbt-core

pip3 install dbt-postgres
pip3 install dbt-bigquery
pip3 install dbt-snowflake
pip3 install dbt-databend-cloud

dbt --version

touch .gitignore
echo ".venv/" >> .gitignore
echo "__pycache__/" >> .gitignore
echo "*.pyc" >> .gitignore
echo ".env" >> .gitignore

dbt init

Set env variables for dbt
export $(grep -v '^#' .env | xargs)

pip3 install "setuptools<68"  
