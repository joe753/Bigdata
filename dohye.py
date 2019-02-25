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

conn = get_conn('melondb')


with conn:
    cur = conn.cursor()
    song_sql = "select s.song_no, s.title, s.genre, a.album_id from MS_Song s inner join Album a on s.album_id = a.album_id "
    cur.execute(song_sql)
    songs = cur.fetchall()
    # print(songs)
    print ("Song data collected")

    album_sql = '''select album_id, album_title, album_genre, 
                cast(rating as char(30)) as rating,
                cast(releasedt as char(30)) as releasedt,
                album_comp, entertainment,
                cast(crawldt as char(30)) as crawldt from Album'''
    cur.execute(album_sql)
    albums = cur.fetchall()
    # print(rows2)
    print ("Album data collected")
    albumids = []
    for i in albums:
        albumid = i['album_id']
        albumids.append(albumid)

    albumdetail = {}
    for i in range(len(albumids)):
        albumdetail[albumids[i]] = albums[i]
    # print(albumdetail)
    for song in songs:
        song['albumdetail'] = albumdetail[song['album_id']]
    
    # print(songs)
# # ---------------------bigquery--------------------------------


client = bigquery.get_client(json_key_file='./bigquery.json', readonly = False)
print("identification success")

DATABASE='bqdb'
TABLE='Song3'

if not client.check_table(DATABASE, TABLE):
    print("Create table {}.{}".format(DATABASE, TABLE, file = sys.stderr))

    client.create_table(DATABASE, TABLE, [
        {'name': 'song_no', 'type': 'string', 'description': 'song id'},
        {'name': 'title', 'type': 'string', 'description': 'song title'},
        {'name': 'genre', 'type': 'string', 'description': 'song genre'},
        {'name': 'album_id', 'type':'string', 'description': 'album id'},
        {'name': 'albumdetail', 'type': 'record', 'description': 'album detail',
          'fields': [{'name': 'album_id', 'type': 'string'},
                     {'name': 'album_title', 'type': 'string'},
                     {'name': 'album_genre', 'type': 'string'},
                     {'name': 'rating', 'type': 'float', 'description': 'album rating'},
                     {'name': 'releasedt', 'type':'date'},
                     {'name': 'album_comp', 'type':'string'},
                     {'name': 'entertainment', 'type':'string'},
                     {'name': 'crawldt', 'type':'timestamp'}

                    ]}])

pushResult = client.push_rows(DATABASE, TABLE, songs, insert_id_key='song_no')
print("Pushed Result is", pushResult)