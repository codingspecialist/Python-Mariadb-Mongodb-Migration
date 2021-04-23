from pymongo import MongoClient
from pymongo.cursor import CursorType
from pymysql import connect
# import json


def mongo_find(mongo, condition=None, db_name=None, collection_name=None):
    result = mongo[db_name][collection_name].find(
        condition, no_cursor_timeout=True, cursor_type=CursorType.EXHAUST)

    # 아이디 안뽑고 싶으면 _id:False 붙이기
    # result = mongo[db_name][collection_name].find(
    #     condition, {"_id": False}, no_cursor_timeout=True, cursor_type=CursorType.EXHAUST)
    return result


def mysql_find(mysql_cursor):
    # 연관관계가 있다면 무조건 ORM으로 해야함. 장고 사용하던지 스프링부트 사용해야함. 아니면 굉장히 까다로움.
    sql = "SELECT * FROM users"
    mysql_cursor.execute(sql)
    # (('id', 8, None, 20, 20, 0, False), ('password', 253, None, 255, 255, 0, True), ('username', 253, None, 255, 255, 0, True))
    # print(mysql_cursor.description)
    # enumerate 몇번째 반복문인지 확인할 때 사용 (1,"1234","ssar")
    # r = [dict((mysql_cursor.description[i][0], value)
    #           for i, value in enumerate(row)) for row in mysql_cursor.fetchall()]

    my_list = []

    for row in mysql_cursor.fetchall():
        my_dict = {}
        for i, value in enumerate(row):
            my_dict[mysql_cursor.description[i][0]] = value

        my_list.append(my_dict)
    return my_list


def mongo_save(mongo, datas, db_name=None, collection_name=None):
    result = mongo[db_name][collection_name].insert_many(datas).inserted_ids
    return result


# Mongo 연결
mongo = MongoClient("localhost", 27017)
# print(mongo)  # connect=True
collection = mongo_find(mongo, None, "test", "users")

# MySQL 연결
mysql = connect(host="localhost", user="root",
                password="cos1234", db="cos", charset="utf8")
mysql_cursor = mysql.cursor()

# mysql 데이터 찾기
rs = mysql_find(mysql_cursor)

# mongo에 데이터 입력하기
mongo_save(mongo, rs, "test", "users")  # List안에 dict을 넣어야 함

# mongo에 데이터 확인하기
for document in collection:
    print(document)  # dict
# json_result = json.dumps(rs, indent=4)  # indent 들여쓰기
