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
    sql_Song = "select s.song_no, s.title, s.genre, a.album_title from MS_Song s inner join Album a on s.album_id = a.album_id "
    cur.execute(sql_Song)
    row = cur.fetchall()
    print ("== Finished getting data from mysql(table = MS_Song) ==")

    sql_Album = '''select album_id, album_title, album_genre, 
                cast(rating as char(20)) as rating,
                cast(releasedt as char(30)) as releasedt,
                album_comp, entertainment,
                cast(crawldt as char(30)) as crawldt from Album'''
    cur.execute(sql_Album)
    row2 = cur.fetchall()
    print (row2)
    print ("== Finished getting data from mysql(table = Album) ==")



# for i in row2:
#     i['rating'] = float(i['rating'])
#     i['releasedt'] = str(i['releasedt'])
#     i['crawldt'] = str(i['crawldt'])



client = bigquery.get_client(json_key_file='./bigquery.json', readonly=False)
DATABASE = "bqdb"
TABLE = "Song"


if not client.check_table(DATABASE, TABLE):
    print("Create table {0}.{1}".format(DATABASE, TABLE), file=sys.stderr)

    client.create_table(DATABASE, TABLE, [
        {'name': 'song_no', 'type': 'string', 'description': 'song id'},
        {'name': 'title', 'type': 'string', 'description': 'song title'},
        {'name': 'genre', 'type': 'string', 'description': 'song genre'},
        {'name': 'album_title', 'type': 'string', 'description': 'album title'}
    ])

pushResult = client.push_rows(DATABASE, TABLE, row, insert_id_key='songno')


print("Pushed Result is", pushResult)


TABLE2 = "Album"
if not client.check_table(DATABASE, TABLE2):
    print("Create table {0}.{1}".format(DATABASE, TABLE2), file=sys.stderr)

    client.create_table(DATABASE, TABLE2, [
        {'name': 'album_id', 'type': 'string', 'description': 'album id'},
        {'name': 'album_title', 'type': 'string', 'description': 'album title'},
        {'name': 'album_genre', 'type': 'string', 'description': 'album genre'},
        {'name': 'rating', 'type': 'float', 'description': 'rating'},
        {'name': 'releasedt', 'type': 'date', 'description': 'releasedt'},
        {'name': 'album_comp', 'type': 'string', 'description': 'album_comp'},
        {'name': 'entertainment', 'type': 'string', 'description': 'entertainment'},
        {'name': 'crawldt', 'type': 'timestamp', 'description': 'crawldt'}
    ])

pushResult2 = client.push_rows(DATABASE, TABLE2, row2, insert_id_key='album_id')


print("Pushed Result is", pushResult2)








