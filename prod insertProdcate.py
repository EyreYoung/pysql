import pymysql
import json


def connect_db():
    return pymysql.connect(host='localhost',
                           port=3306,
                           user='root',
                           password='1996',
                           database='scrm')


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
        asin = prod.get('asin')
        cate = prod['categories']

        prod_cate = []
        for categoryList in prod['categories']:
            count2 += 0.1

            count3 = 0
            for category in categoryList:

                count3 += 0.01
                line = (asin, category)
                if line not in prod_cate:
                    prod_cate.append((asin, category))
                
                print(filename, count1 + count2)

        # prod_cate = list(set(prod_cate))
        print(asin, ",", prod_cate)
        insert_prod_cata(db, prod_cate)
        # if count1 >=100: break

    db.close()
    print("数据插入结束")


read_prod("meta_Electronics")
read_prod("meta_Grocery_and_Gourmet_Food")
read_prod("meta_Health_and_Personal_Care")
read_prod("meta_Home_and_Kitchen")
read_prod("meta_Sports_and_Outdoors")

