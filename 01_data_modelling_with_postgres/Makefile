init:
	python -m venv venv-sparkify
	source venv-sparkify/bin/activate.sh

init_fish_shell:
	python -m venv venv-sparkify
	source venv-sparkify/bin/activate.fish

install_deps:
	pip install -r requirements.txt

run: reset_tables run_etl

reset_tables:
	python create_tables.py
run_etl:
	python etl.py
