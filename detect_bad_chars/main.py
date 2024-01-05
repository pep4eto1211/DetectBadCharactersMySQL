from typing import Optional
from typing_extensions import Annotated
import mysql.connector

from rich import print
import typer

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)

def is_unusual_latin1(sequence: bytearray):
    """Check if the sequence contains unusual Latin-1 characters."""
    if sequence is None:
        return False
    for char in sequence:
        if (char > 255) or (char >= 128 and char <= 159):
            print('Offending char found:')
            print(char)
            return True
    return False

def is_unusual_cp1252(sequence: bytearray):
    """Check if the sequence contains unusual cp1252 characters."""
    if sequence is None:
        return False
    for char in sequence:
        if (char > 255) or (char in [129, 141, 143, 144, 157]):
            print('Offending char found:')
            print(char)
            return True
    return False

# This only works for a single table and a single column. Once we have decided how we can operationalize this we might have
# to find a way to automate it so it goes through multiple ones
@app.command()
def main(mysql_host: Annotated[str, typer.Argument(help='Host address of the MySQL host')],
    mysql_username: Annotated[str, typer.Argument(help='MySQL username')],
    mysql_password: Annotated[str, typer.Argument(help='MySQL password')],
    database: Annotated[str, typer.Argument(help='Database to connect to')],
    table: Annotated[str, typer.Argument(help='Table to check')],
    pk_column: Annotated[str, typer.Argument(help='PK column of the table')],
    offending_column: Annotated[str, typer.Argument(help='Column with issues to check')],
    mysql_port: Annotated[Optional[int], typer.Argument(help='MySQL port')] = 3306,
    bookmark_column: Annotated[str, typer.Option('--bookmark_column', '-b', help='Column that can be used for narrowing down scanned records')] = None,
    bookmark_value: Annotated[str, typer.Option('--bookmark_value', '-v', help='Value of the bookmark column to be used when filtering data')] = None,
):
    cnx = mysql.connector.connect(user=mysql_username, password=mysql_password, database=database, host=mysql_host)
    cnx.set_charset_collation('latin1')
    cursor = cnx.cursor()
    # This query is far too simple and we can benefit from narrowing down the number of records scanned as if there is no bookmark column we might to ave go through the entire table
    query = f"SELECT {pk_column}, {offending_column}, convert({offending_column} using binary) as binary_offender FROM {table}"
    if bookmark_column:
        query += f" where {bookmark_column} > '{bookmark_value}'"
    
    ids = []

    print(f'Running: {query}')
    cursor.execute(query)
    # Right now this only prints stuff but we might want to be able to also export it
    for (id, value, binary_value) in cursor:
        if is_unusual_cp1252(binary_value):
            ids.append(id)
            print(f"Id: {id}")
            print(f"{offending_column}: {value}")
            print(f"Decoded {offending_column}: {binary_value.decode('latin1', 'strict')}")
            print(f"{offending_column} bytearray: {binary_value}")
            print(f"Is unusual : {is_unusual_cp1252(binary_value)}")
            print()

    print('Offending IDs:')
    print(tuple(ids))
