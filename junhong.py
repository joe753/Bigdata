import pymysql
from pprint import pprint
import bigquery
import sys
from time import time
from google.cloud import bigquery as bq
import json


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
# with open('./bigquery.json','r') as key_file:
#     json_key = json.load(key_file)
#     pprint(json_key)
client = bigquery.get_client(json_key_file='./bigquery.json', readonly=False)
print(client.project_id)
# projectid = client.project_id
# print(bool(client))
# exit()
DATABASE = 'bqdb'
TABLE = 'Song_junhong'
print(client.check_table(DATABASE, TABLE))
if not client.check_table(DATABASE, TABLE):
    a=client.create_table(DATABASE, TABLE,  [{"name": "songNo", 
                                            "type": "STRING"},
                                           {"name": "songTitle",
                                            "type": "STRING"},
                                           {"name": "genre",
                                            "type": "STRING"},
                                           {"name": "album",
                                            "type": "RECORD",
                                            "fields": [{"name": "albumNo",
                                                        "type": "STRING"},
                                                       {"name": "albumTitle",
                                                        "type": "STRING"},
                                                      
                                                      ]
                                            }
                                           ]
                       )
    print(a)
    print("Create table {0}.{1}".format(DATABASE, TABLE), file=sys.stderr)
else:
    print("Table {} already exsits".format(TABLE))
start2 = time()
with conn:
    cur = conn.cursor()
    for k in range(10):
        sql = """select s.song_no, s.title,s.genre, a.* 
                from MS_Song s inner join Album a on s.album_id = a.album_id
                limit %s offset %s""" % (str(100000),str(100000*k))
        cur.execute(sql)
        rows = cur.fetchall()
        partlst = []
        if not rows:
            print("break!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            break
        else:
            for i, row in enumerate(rows):
                songdic ={'songNo': row['song_no'],
                          'songTitle': row['title'],
                          'genre': row['genre'],
                          'album': {'albumNo':row['album_id'], 
                                    'albumTitle':row['album_title']}, 
                          }
                partlst.append(songdic)
            print (partlst)
            pushResult = client.push_rows(DATABASE, TABLE, partlst, insert_id_key='songNo')
            print("Pushed Result is ", pushResult) 
end2 = time()
print(end2-start2) # 0.87s