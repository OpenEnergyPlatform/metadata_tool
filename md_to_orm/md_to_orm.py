import getpass
import json
import logging
import oedialect
import os
import pathlib
from collections import namedtuple
from typing import List, Optional

import jmespath
import sqlalchemy as sa
import typer
from geoalchemy2.types import Geometry

from postgresql_types import TYPES

CONNECTION_STRING = "{engine}://{user}:{token}@{host}"

DB = namedtuple("Database", ["engine", "metadata"])


class CredentialError(Exception):
    pass


class DatabaseError(Exception):
    pass


class MetadataError(Exception):
    pass


def setup_db_connection(
    engine: str, host: str, port: Optional[int] = None, database: Optional[str] = None
) -> DB:
    try:
        user = os.environ["DB_USER"]
    except KeyError:
        user = input("Enter OEP-username:")
    try:
        token = os.environ["DB_TOKEN"]
    except KeyError:
        token = getpass.getpass("Token:")

    # Generate connection string:
    conn_str = CONNECTION_STRING
    if port:
        conn_str += ":{port}"
    if database:
        conn_str += "/{database}"
    conn_str = conn_str.format(
        engine=engine, user=user, token=token, host=host, port=port, database=database
    )

    engine = sa.create_engine(conn_str)
    metadata = sa.MetaData(bind=engine)
    return DB(engine, metadata)


def create_schema(db: DB, schema: str = "public"):
    try:
        db.engine.execute(sa.schema.CreateSchema(schema))
    except sa.exc.ProgrammingError:
        pass


def create_tables(db: DB, tables: List[sa.Table]):
    for table in tables:
        if any([isinstance(column.type, Geometry) for column in table.columns]):
            try:
                db.engine.execute("CREATE EXTENSION POSTGIS;")
            except sa.exc.ProgrammingError as e:
                if "psycopg2.errors.DuplicateObject" in e.args[0]:
                    pass
                else:
                    logging.error(
                        f'Cannot create extension "POSTGIS" needed for table "{table.name}"'
                    )
                    raise
        if not db.engine.dialect.has_schema(db.engine, table.schema):
            create_schema(db, table.schema)
        if not db.engine.dialect.has_table(db.engine, table.name, table.schema):
            try:
                table.create()
                logging.info(f"Created table {table.name}")
            except sa.exc.ProgrammingError:
                logging.error(f'Table "{table.name}" already exists')
                raise


def order_tables_by_foreign_keys(tables: List[sa.Table]):
    """
    This function tries to order tables to avoid missing foreign key errors.

    By now, ordering is simply done by counting of foreign keys.
    """
    return sorted(tables, key=lambda x: len(x.foreign_keys))


def create_tables_from_metadata_file(db: DB, metadata_file: str) -> List[sa.Table]:
    with open(metadata_file, "r") as metadata_json:
        metadata = json.loads(metadata_json.read())
    tables_raw = jmespath.search("resources", metadata)

    tables = []
    for table in tables_raw:
        # Get (schema) and table name:
        schema_table_str = table["name"].split(".")
        if len(schema_table_str) == 1:
            schema = "public"
            table_name = schema_table_str[0]
        elif len(schema_table_str) == 2:
            schema, table_name = schema_table_str
        else:
            raise MetadataError("Cannot read table name (and schema)", table["name"])

        # Get primary keys:
        primary_keys = jmespath.search("schema.primaryKey[*]", table)

        # Get foreign_keys:
        foreign_keys = {
            fk["fields"][0]: fk["reference"]
            for fk in jmespath.search("schema.foreignKeys", table)
        }

        # Create columns:
        columns = []
        for field in jmespath.search("schema.fields[*]", table):
            # Get column type:
            try:
                column_type = TYPES[field["type"]]
            except KeyError:
                raise MetadataError(
                    "Unknown column type", field, field["type"], metadata_file
                )

            if field["name"] in foreign_keys:
                foreign_key = foreign_keys[field["name"]]
                column = sa.Column(
                    field["name"],
                    column_type,
                    sa.ForeignKey(
                        f'{foreign_key["resource"]}.{foreign_key["fields"][0]}'
                    ),
                    primary_key=field["name"] in primary_keys,
                    comment=field["description"],
                )
            else:
                column = sa.Column(
                    field["name"],
                    column_type,
                    primary_key=field["name"] in primary_keys,
                    comment=field["description"],
                )
            columns.append(column)

        tables.append(sa.Table(table_name, db.metadata, *columns, schema=schema))
    return tables


def main(
    metadata_files: List[str],
    from_folder: bool = typer.Option(False, help="Read metadate from folder"),
    engine: str = typer.Option(
        "postgresql+oedialect", help="SQLAlchemy engine", show_default=True
    ),
    host: str = typer.Option("openenergy-platform.org", help="Host", show_default=True),
    port: Optional[int] = typer.Option(None, help="Optional: Port"),
    database: Optional[str] = typer.Option(None, help="Optional: Database name"),
    log_level: str = typer.Option("INFO", help="Logging level", show_default=True),
):
    """
    Creates database tables from metadata json files

    METADATA_FILES can be single filename, list of filenames or
    (if option "--from-folder" is used) a folder containing json metadata files.
    All tables from metadata are create via sqlalchemy ORM and uploaded.
    If needed, schema and POSTGIS extension are created.
    See "postgresql_types" for availalbe column types.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.getLevelName(log_level.upper()))

    if from_folder:
        folder = pathlib.Path.cwd() / metadata_files[0]
        metadata_files = [str(file) for file in folder.iterdir()]

    db = setup_db_connection(engine, host, port, database)

    tables = []
    for metadata_file in metadata_files:
        try:
            md_tables = create_tables_from_metadata_file(db, metadata_file)
        except:
            logger.error(
                f'Could not generate tables from metadatafile "{metadata_file}"'
            )
            raise
        tables.extend(md_tables)
    ordered_tables = order_tables_by_foreign_keys(tables)
    create_tables(db, ordered_tables)


if __name__ == "__main__":
    typer.run(main)
