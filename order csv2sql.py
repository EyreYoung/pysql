import pymysql
import csv


def connect_db():
    return pymysql.connect(host='localhost',
                           port=3306,
                           user='root',
                           password='1996',
                           database='scrm')


def read_csv(path):
    order_csv = csv.reader(open(path, 'r'))
    db = connect_db()
    count = 0
    orders = []

    for line in order_csv:
        # print(line)
        orders.append((line[0], line[1], line[2], line[3]))

        count += 1
        print(count)
        # if count >= 5: break

    insert_order(db, orders)

    db.close()


def insert_order(db, orders):
    sqlstr = """insert into orders values(%s, %s, %s, %s)"""
    # db = connect_db()
    cursor = db.cursor()

    try:
        # 执行sql语句
        cursor.executemany(sqlstr, orders)
        assert cursor.rowcount == len(orders), '插入数据数量有误，应为%s，实为%s'%(len(orders), cursor.rowcount)
        # 提交到数据库执行
        db.commit()
        print('数据插入完成')
    except Exception as e:
        # 如果发生错误则回滚
        print(e)
        db.rollback()


read_csv("order/ratings_Sports_and_Outdoors.csv")
