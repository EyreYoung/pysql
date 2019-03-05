import pymysql
import json

def connect_db():
    return pymysql.connect(host='localhost',
                           port=3306,
                           user='root',
                           password='1996',
                           database='scrm')


def query_prod():
    sqlstr = ("")
    db = connect_db()
    cursor = db.cursor()
    cursor.execute("select version()")
    data = cursor.fetchone()
    db.close()
    print(data)


def query_json(path):
    count = 0
    prodjson = open(path)
    for line in prodjson:
        # line = line.replace('\'', '\"')
        # prod = json.loads(line)
        # print(line)
        # print(isinstance(line, str))

        yield json.dumps(eval(line))


filename = "meta_Sports_and_Outdoors"
path = "prod/" + filename + ".json"

f = open(filename + ".strict", "w")

count = 0
for l in query_json(path):
    count += 1
    f.write(l + '\n')
    print(count)
f.close()

# new = open("output.strict")
# for l in new:
#     prod = json.loads(l)
#     print(prod['asin'])

