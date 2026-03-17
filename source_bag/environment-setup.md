python3 -m venv .venv

pip3 install dbt-core
pip3 install dbt-snowflake
pip3 install dbt-databend-cloud
pip3 install snowflake-connector-python pymysql python-dotenv
pip3 install jupyter nbconvert papermill

dbt --version

touch .gitignore
echo ".venv/" >> .gitignore
echo "__pycache__/" >> .gitignore
echo "*.pyc" >> .gitignore
echo ".env" >> .gitignore

dbt init

# Create env file
cp .env.example .env
# Set env variables for dbt
export $(grep -v '^#' .env | xargs)

pip3 install "setuptools<68"  


# Github pipeline
mkdir -p .github/workflows
nano .github/workflows/ci.yml
