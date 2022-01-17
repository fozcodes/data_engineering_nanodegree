init:
	python -m venv venv-sparkify

install_deps:
	pip install -r requirements.txt

run: reset_tables run_etl

reset_tables:
	python create_tables.py
run_etl:
	python etl.py
