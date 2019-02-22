import pymysql
import bigquery
import sys


def get_conn(db):
    return pymysql.connect(
        host='34.85.92.216',
        user='root',
        password='gusdnr75',
        port=3306,
        db=db,
        cursorclass=pymysql.cursors.DictCursor,
        charset='utf8')

conn_dooodb = get_conn('melondb')

with conn_dooodb:
    cur = conn_dooodb.cursor()
    sql_Song = '''select s.song_no, s.title, s.genre, a.album_id, a.album_title, a.album_genre ,
                    cast(a.rating as char(20)) as rating,
                cast(a.releasedt as char(30)) as releasedt,
                a.album_comp, a.entertainment,
                cast(a.crawldt as char(30)) as crawldt 
                from MS_Song s inner join Album a on s.album_id = a.album_id'''
    cur.execute(sql_Song)
    row = cur.fetchall()
    print ("== Finished getting data from mysql( table = MS_Song & Album ) ==")

data = []
for i in row:
    datum =  {'song_no' : i['song_no'] ,
              'title' : i['title'],
              'genre' : i['genre'],
              'album' : {'album_id' : i['album_id'],
                        'album_title' : i['album_title'],
                        'album_genre' : i['album_genre'],
                        'rating' : i['rating'],
                        'releasedt' : i['releasedt'],
                        'album_comp' : i['album_comp'],
                        'entertainment' : i['entertainment'],
                        'crawldt' : i['crawldt']}
            }
    data.append(datum)


client = bigquery.get_client(json_key_file='./bigquery.json', readonly=False)
DATABASE = "bqdb"
TABLE = "Song2"

print (data)
if not client.check_table(DATABASE, TABLE):

    print("Create table {0}.{1}".format(DATABASE, TABLE), file=sys.stderr)
    client.create_table(DATABASE, TABLE, [
        {'name': 'song_no', 'type': 'string', 'description': 'song id'},
        {'name': 'title', 'type': 'string', 'description': 'song title'},
        {'name': 'genre', 'type': 'string', 'description': 'song genre'},
        {'name': 'album', 'type': 'record', 'description': 'record', 
        "fields" :  [{'name': 'album_id', 'type': 'string', 'description': 'album id'},
                    {'name': 'album_title', 'type': 'string', 'description': 'album title'},
                    {'name': 'album_genre', 'type': 'string', 'description': 'album genre'},
                    {'name': 'rating', 'type': 'float', 'description': 'rating'},
                    {'name': 'releasedt', 'type': 'date', 'description': 'releasedt'},
                    {'name': 'album_comp', 'type': 'string', 'description': 'album_comp'},
                    {'name': 'entertainment', 'type': 'string', 'description': 'entertainment'},
                    {'name': 'crawldt', 'type': 'timestamp', 'description': 'crawldt'} ]
        }
    ])
else:
    print("already exists")

pushResult = client.push_rows(DATABASE, TABLE, data, insert_id_key='song_no')

print("Pushed Result is", pushResult)








