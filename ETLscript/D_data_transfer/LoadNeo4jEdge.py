import csv
from D_data_transfer.MySQLManager import *

def save_coop_to_csv():
    """
    将mysql中的cooperation信息以能导入neo4j的格式存储为csv文件
    :return:
    """
    mysql = MysqlManager()
    sql = 'select actor_name1, actor_name2 from cooperate_with order by id'
    results = mysql.execute_query(sql)
    # print(results)
    # print('')
    # print(type(results))

    for result in results:
        print(result)
        headers = [':START_ID', ':END_ID', ':TYPE']
        rows = []
        sql = 'select id from actor where name=%s'
        id1 = mysql.execute_query_with_para(sql, result['actor_name1'])
        id2 = mysql.execute_query_with_para(sql, result['actor_name2'])
        row = {
            ':START_ID': id1,
            ':END_ID': id2,
            ':TYPE': 'cooperate_with'
        }
        print(row)
        rows.append(row)

    # 将存储好的数据列表导入csv
    # with open('Citations.csv', 'w') as _csv:
    #     f_scv = csv.DictWriter(_csv, headers)
    #     f_scv.writeheader()
    #     f_scv.writerows(rows)
    #     print('存储到csv成功')


def main():
    start = datetime.datetime.now()

    save_coop_to_csv()

    end = datetime.datetime.now()
    print("调度总时长：", end - start)


if __name__ == '__main__':
    main()