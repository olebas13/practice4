import pymysql
import pymysql.cursors
import csv
import sys
from decimal import Decimal, DecimalException

connection = None

try:
    conn = pymysql.connect(
        host = 'helensbi.mysql.tools',
        user = 'helensbi_cheminf',
        password = 'pMnh9J778KMv',
        db = 'helensbi_db',
        cursorclass = pymysql.cursors.DictCursor
    )
    connection = conn
except pymysql.err.InternalError:
    print("Помилка підключення до бази. Перевірте параметри підключення")
    sys.exit()
except pymysql.err.OperationalError:
    print("Помилка підключення до бази. Перевірте параметри підключення")
    sys.exit()
except RuntimeError:
    print("Помилка підключення до бази. Перевірте параметри підключення")
    sys.exit()

try:
    CSV_FILENAME = input("Введіть назву CSV-файла в форматі <назва>.csv: ")
except FileNotFoundError:
    print("Файл не найдено. Файл має знаходитись в директорії з програмою")
except KeyError:
    print("Невірний формат файла. Файл з даними має бути в форматі CSV")

csv_smile = None
csv_name = None
csv_inc = None
csv_mass = None
csv_logP = None
csv_cas = None
csv_mdl = None

db_struct_id = None
db_smile = None
db_react_id = None
db_conversion = None


with open(CSV_FILENAME, "r", newline = "") as file:
    reader = csv.DictReader(file)
    for row in reader:
        try:
            csv_smile = row['smile']
        except KeyError:
            sys.exit("Немає SMILE структури в файлі (smile)")
        
        try:
            csv_name = row['name']
        except KeyError:
            print("Невідома назва структури (name)")

        try:
            csv_inc = row['inChi']
        except KeyError:
            print("Невідомий ідентифікатор структури (inChi)")

        try:
            csv_mass = Decimal(row['mass'])
        except KeyError:
            print("Невідома молярна маса структури (mass)")
        except DecimalException:
            print("Некоректний тип значення молярної масси (mass)")

        try:
            csv_logP = Decimal(row['logP'])
        except KeyError:
            print("Невідомий коефіцієнт розподілу (logP)")
        except DecimalException:
            print("Некоректний тип значення коефіцієнта розподілу (logP)")

        try:
            csv_cas = int(row['cas'])
        except KeyError:
            print("Невідомий CAS-номер структури (cas)")
        except ValueError:
            print("Некоректний тип значення СAS-номеру. Згідно схеми бази данних, тип поля cas в таблиці ci_structures INTEGER")

        try:
            csv_mdl = Decimal(row['mdl'])
        except KeyError:
            print("Невідомий mdl структури")
        except DecimalException:
            print("Некоректний тип значення mdl структури")


query_select = "select * from ci_structures where smile = %s"
insert = "insert into ci_structures (smile, name, inChi, mass, logP, cas, mdl) values (%s, %s, %s, %s, %s, %s, %s)"
query_for_reaction = "select * from ci_reactions where resStrucID = %s and conversion = (select max(conversion) from ci_reactions where resStrucID = %s)"

try:
    cursor = connection.cursor()
    result = cursor.execute(query_select, (csv_smile))
    for row1 in cursor:
        db_struct_id = row1['id']
        db_smile = row1['smile']
    
    if result == 0:
        cursor.execute(insert, (csv_smile, csv_name, csv_inc, csv_mass, csv_logP, csv_cas, csv_mdl))
        cursor.execute(query_select, (csv_smile))
        for row2 in cursor:
            print("Структура внесена в таблицю ci_structure з id = " + str(row2['id']))
    else:
        print("Структура " + db_smile + " вже наявна в базі")
        res = cursor.execute(query_for_reaction, (db_struct_id, db_struct_id))
        if res == 0:
            print("Реакцій з отримання данної структури немає.")
        else:
            for row3 in cursor:
                db_react_id = row3['id']
                db_conversion = row3['conversion']

            print("Реакція з найкращою конверсією - id = " + str(db_react_id) + ", конверсія - " + str(db_conversion))   
finally:
    connection.commit()
    connection.close()
