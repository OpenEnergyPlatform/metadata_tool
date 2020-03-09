# MD to ORM

Create database tables (and schema) from metadata json file(s)

CMD Tool is based on Typer: https://typer.tiangolo.com/

## Usage:
See help: 
```
python md_to_orm.py --help
```

Try it with example files in "test_files" folder:
```
python md_to_orm.py --engine=postgresql --host=localhost --port=5432 --log-level=debug --from-folder test_files
```