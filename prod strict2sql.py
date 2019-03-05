import pymysql
import json


def connect_db():
    return pymysql.connect(host='localhost',
                           port=3306,
                           user='root',
                           password='1996',
                           database='scrm')


def insert_prod(db, asin, title, price, imUrl, brand, cate):
    sqlstr = """insert into product values('%s', '%s', '%s', '%s', '%s', '%s')""" % (asin, title, price, imUrl, brand, cate)
    # db = connect_db()
    cursor = db.cursor()

    print(asin, title, price, imUrl, brand, cate)

    try:
        # 执行sql语句
        cursor.execute(sqlstr)
        # 提交到数据库执行
        db.commit()
    except:
        # 如果发生错误则回滚
        db.rollback()

    # db.close()


def insert_prod_cata(db, T):
    sqlstr = """insert into prodcate values (%s, %s)"""
    # db = connect_db()
    cursor = db.cursor()

    try:
        # 执行sql语句
        cursor.executemany(sqlstr, T)
        # assert cursor.rowcount == len(T), 'my error message'
        # 提交到数据库执行
        db.commit()
    except Exception as e:
        # 如果发生错误则回滚
        print(e)
        db.rollback()

    # db.close()


def read_prod(filename):
    path = filename + ".strict"
    f = open(path)

    db = connect_db()

    count1 = 0
    for l in f:
        count1 += 1
        count2 = 0
        prod = json.loads(l)
        asin = prod.get('asin', "null")
        title = pymysql.escape_string(prod.get('title', "null"))
        price = prod.get('price', "0.00")
        imUrl = prod.get('imUrl', "null")
        brand = prod.get('brand', "null")
        cate = prod['categories'][0][0]

        insert_prod(db, asin, title, price, imUrl, brand, cate)

        prod_cate = []
        for category in prod['categories'][0]:
            count2 += 0.1

            prod_cate.append((asin, category))
            # insert_prod_cata(db, asin, category)

            print(filename, count1 + count2)

        insert_prod_cata(db, prod_cate)


    db.close()


read_prod("meta_Health_and_Personal_Care")
read_prod("meta_Home_and_Kitchen")
read_prod("meta_Sports_and_Outdoors")



