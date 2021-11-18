import pymysql, os, datetime, logging, base64
from mutagen import File
from mutagen.mp3 import MP3
from mutagen.id3 import ID3, PictureType
from mutagen.flac import FLAC, Picture
from mutagen.oggvorbis import OggVorbis

def cursor(param, check_is_true):

    conn = pymysql.connect(host='127.0.0.1',
                        port=3306,
                        user='User',
                        passwd="Password",
                        db='music_database')

    commit = "commit;"

    cur = conn.cursor()
    print(param)
    cur.execute(param)

    if check_is_true == True:
        data = cur.fetchall()
        cur.close()
        return data

    else:
        print("Committing\n")
        cur.execute(commit)
        cur.close()
        return

def create_table():

    drop = """DROP TABLE IF EXISTS music;"""

    create = """CREATE TABLE music (id int(6) NOT NULL AUTO_INCREMENT, artist varchar(256) NOT NULL, title varchar(256) NOT NULL, album varchar(256) DEFAULT NULL, length time(6) DEFAULT NULL, genre varchar(255) DEFAULT NULL, bitrate varchar(25) NOT NULL, filename varchar(255) NOT NULL, has_art boolean NOT NULL, file_type varchar(6) NOT NULL, PRIMARY KEY (id));"""

    cursor(drop, False)
    cursor(create, False)
    return

def exists(artist, title, album, genre, time, filename, file_type, has_art):

    try:
        location = "/mnt/e/Backups/Edans_Media/Music/{}".format(song)
        track = File(location)
        time = str(datetime.timedelta(seconds=track.info.length))
        query = 'SELECT * FROM music WHERE artist = "{0}" AND title = "{1}" AND album = "{2}" AND genre = "{3}" AND length = "{4}" AND bitrate = "{5}" AND filename = "{6}" AND file_type = "{7}" AND has_art = "{8}";'.format(artist, title, album, genre, time, str(track.info.bitrate // 1000) + "kpbs", filename, file_type, has_art)
        if str(title) in cursor(query, True):
            print("Exists:" + "True")
            return True
        else:
            print("Exists " + "False")
            return False
    except Exception as e:
        print("Exists: " + str(e))
        return False

# Take str as input, adds a '\' prior to illegal char to nullify entry obstruction in MySQL
def sanitize_input(input):
    try:
        illegal = ['\\', '%', '"']
        input = input.strip()
        i_log = [i for i, letter in enumerate(input) if letter in illegal]

        if len(i_log) < 0:
            error = "Invalid input"
            return error
        else:
            # Reversed as to not change index values upon changes made per iteration
            for i in reversed(i_log):
                input = input[:i] + '\\' + input[i:]
                logging.error(" " + str(song) + " failed: " + str(input))
            return input
    except Exception as e:
        logging.error(" " + str(song) + " failed: " + str(e) + input)

# Takes filename of track and MP3 filetype
# Extracts Album Art and places into /cove directory
# Enters entry (artist, title, album, genre, length, bitrate, whether_has_art, filename) in to MysQL
def mysql_insert_mp3(song, file_type):
        
    location = "/mnt/e/Music/{}".format(song)
    try:
        track = File(location)
    except Exception as e:
        logging.fatal("\n " + str(song) + " critically failed: " + str(e) + "\n")
        return
    has_art = 0
    work = {}

    try:
        artist = sanitize_input(track.tags['TPE1'][0])
    except Exception as e:
        logging.error(" " + str(song) + " failed: " + str(e))
        artist = """"""
    try:
        title = sanitize_input(track.tags['TIT2'][0])
    except Exception as e:
        logging.error(" " + str(song) + " failed: " + str(e))
        title = """"""
    try:
        album = sanitize_input(track.tags['TALB'][0])
    except Exception as e:
        logging.error(" " + str(song) + " failed: " + str(e))
        album = """"""
    try:
        genre = sanitize_input(track.tags['TCON'][0])
    except Exception as e:
        logging.error(" " + str(song) + " failed: " + str(e))
        genre = """"""
    time = str(datetime.timedelta(seconds=track.info.length))


    if exists(artist, title, album, genre, time, song, file_type, has_art) == False: #TODO: Fix exists()
        try:
            if os.path.exists(location + " Cover Art.jpg") == False:
                artwork = track.tags['APIC:'].data
                with open("/mnt/e/Cover_Art/{}".format(song + " Cover Art.jpg"), 'wb') as img:
                    print("/mnt/e/Cover_art/{}".format(song + " Cover Art.jpg"))
                    img.write(artwork)
                    has_art = 1
            else:
                has_art = 1
        except Exception as e:
            logging.error(" " + str(song) + " failed: " + str(e))
            pass  
        query = '''INSERT INTO music (artist, title, album, length, genre, bitrate, filename, file_type, has_art) VALUES ("{0}", "{1}", "{2}", "{3}", "{4}", "{5}", "{6}", "{7}", {8});'''.format(artist, title, album, time, genre, str(track.info.bitrate // 1000) + "kpbs", song, file_type, has_art)
        cursor(query, False)
        print("\n")

    else:
        print("Exists", "\n")

    return

# Takes filename of track and FLAC filetype
# Extracts Album Art and places into /cove directory
# Enters entry (artist, title, album, genre, length, bitrate, whether_has_art, filename) in to MysQL
def mysql_insert_flac(song, file_type):

    location = "/mnt/e/Music/{}".format(song)
    try:
        track = File(location)
    except Exception as e:
        logging.fatal("\n " + str(song) + " critically failed: " + str(e) + "\n")
        return
    has_art = 0
    work = {}

    try:
        artist = sanitize_input(track.tags['artist'][0])
    except Exception as e:
        logging.error(" " + str(song) + " failed: " + str(e))
        artist = ""
    try:
        title = sanitize_input(track.tags['title'][0])
    except Exception as e:
        logging.error(" " + str(song) + " failed: " + str(e))
        title = ""
    try:
        album = sanitize_input(track.tags['album'][0])
    except Exception as e:
        logging.error(" " + str(song) + " failed: " + str(e))
        album = ""
    try:
        genre = sanitize_input(track.tags['genre'][0])
    except Exception as e:
        logging.error(" " + str(song) + " failed: " + str(e))
        genre = ""
    time = str(datetime.timedelta(seconds=track.info.length))
    
    if exists(artist, title, album, genre, time, song, file_type, has_art) == False: #TODO: Fix exists()
        try:
            if os.path.exists(location + " Cover Art.jpg") == False:
                artwork = track.pictures
                for p in artwork:
                    if p.type == 3:
                        has_art = 1
                        with open("/mnt/e/Cover_Art/{}".format(song + " Cover Art.jpg"), 'wb') as img:
                            print("/mnt/e/Cover_art/{}".format(song + " Cover Art.jpg"))
                            img.write(p.data)
                    else:
                        pass
            else:
                has_art = 1
        except Exception as e:
            logging.error(" " + str(song) + " failed: " + str(e))
            pass
    
        query = '''INSERT INTO music (artist, title, album, length, genre, bitrate, filename, file_type, has_art) VALUES ("{0}", "{1}", "{2}", "{3}", "{4}", "{5}", "{6}", "{7}", {8});'''.format(artist, title, album, time, genre, str(track.info.bitrate // 1000) + "kpbs", song, file_type, has_art)
        cursor(query, False)
        print("\n")

    else:
        print("Exists", "\n")
        
    return

# Takes filename of track and Ogg filetype
# Extracts Album Art and places into /cove directory
# Enters entry (artist, title, album, genre, length, bitrate, whether_has_art, filename) in to MysQL
def mysql_insert_ogg(song, file_type):

    location = "/mnt/e/Music/{}".format(song)
    try:
        track = File(location)
    except Exception as e:
        logging.fatal("\n " + str(song) + " critically failed: " + str(e) + "\n")
        return
    has_art = 0
    work = {}

    try:
        artist = sanitize_input(track.tags['artist'][0])
    except Exception as e:
        logging.error(" " + str(song) + " failed: " + str(e))
        artist = ""
    try:
        title = sanitize_input(track.tags['title'][0])
    except Exception as e:
        logging.error(" " + str(song) + " failed: " + str(e))
        title = ""
    try:
        album = sanitize_input(track.tags['album'][0])
    except Exception as e:
        logging.error(" " + str(song) + " failed: " + str(e))
        album = ""
    try:
        genre = sanitize_input(track.tags['genre'][0])
    except Exception as e:
        logging.error(" " + str(song) + " failed: " + str(e))
        genre = ""
    time = str(datetime.timedelta(seconds=track.info.length))

    if exists(artist, title, album, genre, time, song, file_type, has_art) == False: #TODO: Fix exists()
        try:
            if os.path.exists(location + " Cover Art.jpg") == False:
                artwork = base64.b64decode(track.tags['metadata_block_picture'][0])
                with open("/mnt/e/Cover_Art/{}".format(song + " Cover Art.jpg"), 'wb') as img:
                    print("/mnt/e/Cover_art/{}".format(song + " Cover Art.jpg"))
                    img.write(artwork)
                    has_art = 1
            else:
                has_art = 1
        except Exception as e:
            logging.error(" " + str(song) + " failed: " + str(e))
            pass

        query = '''INSERT INTO music (artist, title, album, length, genre, bitrate, filename, file_type, has_art) VALUES ("{0}", "{1}", "{2}", "{3}", "{4}", "{5}", "{6}", "{7}", {8});'''.format(artist, title, album, time, genre, str(track.info.bitrate // 1000) + "kpbs", song, file_type, has_art)
        cursor(query, False)
        print("\n")

    else:
        print("Exists", "\n")
        
    return

if __name__ == "__main__":

    logging.basicConfig(filename="mp3.log", level=logging.ERROR)

    rootdir = "/mnt/e/Music"
    create_table()

    for root, dirs, filenames in os.walk(rootdir):
        for song in filenames:
            if str(song).endswith(".mp3"):
                print(song)
                mysql_insert_mp3(song, "MP3")
            elif str(song).endswith(".flac"):
                print(song)
                mysql_insert_flac(song, "FLAC")
            elif str(song).endswith(".ogg"):
                print(song)
                mysql_insert_ogg(song, "Ogg")
            else:
                print("Skipping " + song)
                pass
        break
