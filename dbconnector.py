import pymysql.cursors

def getConnection():
    connection = pymysql.connect(
        host = 'localhost',
        user = 'root',
        password = 'olebasfcdk14881927',
        db = 'chemtest',
        cursorclass = pymysql.cursors.DictCursor
    )

    return connection