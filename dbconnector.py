import pymysql.cursors

def getConnection():
    connection = pymysql.connect(
        host = 'helensbi.mysql.tools',
        user = 'helensbi_cheminf',
        password = 'pMnh9J778KMv',
        db = 'helensbi_db',
        cursorclass = pymysql.cursors.DictCursor
    )

    return connection