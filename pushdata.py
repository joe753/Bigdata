import bigquery
import sys

client = bigquery.get_client(json_key_file='./bigquery.json', readonly=False)

DATABASE = "bqdb"
TABLE = "test"


if not client.check_table(DATABASE, TABLE):
    print("Create table {0}.{1}".format(DATABASE, TABLE), file=sys.stderr)

client.create_table(DATABASE, TABLE, [
    {'name': 'songno', 'type': 'string', 'description': 'song id'},
    {'name': 'title', 'type': 'string', 'description': 'song title'},
    {'name': 'albumid', 'type': 'string', 'description': 'album id'}
])
        
ttt = [ {'songno': '222', 'title': '홍2', 'albumid': '121212121'} ]

pushResult = client.push_rows(DATABASE, TABLE, ttt, insert_id_key='songno')

print("Pushed Result is", pushResult)