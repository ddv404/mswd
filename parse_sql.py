# -*- coding:UTF-8 -*-

import sqlparse
from sqlparse.sql import Where, Comparison, Parenthesis, Identifier
import os
import csv
import time

print("""

仅支持INSERT INTO插入语句

""")

sql_file_name = input("请输入文件名：")


if not os.path.exists(sql_file_name) :
    print("当前文件不存在。")
    exit(0)

table_name = input("请输入要输入数据的表名：")

indexs = input("请输入要筛选导出值的下标（多个下标以逗号进行相隔，下标从0开始）：")
indexs = list(map(lambda x: int(x) ,indexs.split(",")))

create_file_time = time.strftime("%Y-%m-%d-%H-%M-%S")
# indexs = [1,3,4]
print('导出文件：%s-%s-%s.csv'%(sql_file_name,table_name,str(create_file_time)))
with open(sql_file_name, encoding='utf-8-sig') as f:
    with open('%s-%s-%s.csv'%(sql_file_name,table_name,str(create_file_time)), 'w', encoding='utf-8-sig', newline='') as ff:
        writer = csv.writer(ff)
        # writer.writerow(project_names)
        for line in f:
            line = line.strip()
            # 如果不是插入语句，则跳过当前语句
            if 'INSERT' not in line and 'insert' not in line and 'INTO' not in line and 'into' not in line:
                continue
            
            # 如果当前插入语句，不是指定表则跳过
            if '`%s`'%(table_name) not in line:
                continue

            ps = sqlparse.parse(line)
            values = ps[0].tokens[6]
            vs = values[2][1].get_identifiers()
            vvs = list(map(lambda x: {'type':x._get_repr_name(),'value':x.value}, vs))

            need_values = []

            for index in indexs:
                # print(index)
                if vvs[index]['type'] == 'Single':
                    val = vvs[index]['value'].replace('\'','')
                    if val == 'NULL':
                        val = ''
                else:
                    val = vvs[index]['value']
                    if val == 'NULL':
                        val = ''

                need_values.append(val)
            print(need_values)
            writer.writerow(need_values)


