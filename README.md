#  DataFactory

The currect idea for this project is to follow the same idea as MS DataFactory.

By providing a source and sync, connection (sql server to sqlite3) as of this writing it can automatically copy all tables needed into a sqlite3 db.

## Setup

In a terminal change to desired folder and run following commands
```
git clone https://github.com/jb-tech1999/DataFactory2.0
```
Change into folder and create a new Python VENV - Windows based
```
python -m venv <venv name>
<venv name>/scripts/activate.ps1
pip install -r requirements.txt
```

After packages have been installed, initial setup of the app.py can begin.

Change the sink_conn variable to your own sqlite database connection.

For the source connection I'm making use of ODBC connections in Windows so make sure to create a new DSN connection and provide the correct name in the app file.

