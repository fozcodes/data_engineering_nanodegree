init:
	python -m venv venv-sparkify

install_deps:
	pip install -r requirements.txt

run: reset_tables_all run_etl_all

reset_tables_all:
	python create_tables.py --table-types staging,datamart

run_etl_all:
	python etl.py --table-types staging,datamart

reset_tables_staging:
	python create_tables.py --table-types staging

run_etl_staging:
	python etl.py --table-types staging

reset_tables_datamart:
	python create_tables.py --table-types datamart

run_etl_datamart:
	python etl.py --table-types datamart
