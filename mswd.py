# encoding: utf-8
# -*- coding: utf-8 -*-

import csv
import os
import sqlite3
import datetime
import uuid
import time
import prettytable as pt
import copy


db_file_name = "ddv.db"

# 设置进行关联的字段
# 指定关联的key
# 建议设置可以表示用户唯一性的字段
links = ['name','idCard','phone','身份证','姓名','手机号','电话']


def init_db():
    
    if not os.path.exists(db_file_name) :
        print("初始化db。。。")
        conn = sqlite3.connect(db_file_name)
        cursor = conn.cursor()

        # 创建数据表
        sql = "CREATE table if not exists mswd(id integer PRIMARY KEY autoincrement,name varchar(256), val varchar(256), uuid varchar(32), create_time integer ,project varchar(256),other_project varchar(2560) DEFAULT 'ddv')"
        cursor.execute(sql)
            
        conn.commit()
        conn.close()


# 判断数据是否已经存在
# 并获取对应的uuid
# 对当前条数据进行查询并
def check_data_if_exists(cells):
    # 获取所有的val值
    # 值获取需要进行关联的name的val
    # print(links)
    valls = list(map(lambda x: x['val'], filter(lambda y: y['name'] in links, cells)))
    conn = sqlite3.connect(db_file_name)
    cursor = conn.cursor()
    values = cursor.execute('select val,uuid from mswd where val in ( %s )' % (','.join(['?'] * len(valls)  ) ),(tuple(valls))  )
    values = list(map(lambda x:{
        'val':x[0],
        'uuid':x[1]
    },values))
    conn.commit()
    conn.close()
    return values


# 读取csv文件
def read_csv(csv_file_name, code_type='utf-8-sig'):
    global not_links
    try:
        with open(csv_file_name, encoding=code_type) as f:
            reader = csv.reader(f)
            header = next(reader)
            print(header)
            # 该处加入手动输入确认
            yn = input("请确认字段定义行（Y or N）：")
            if yn != "Y" and yn != "y":
                print("请确认字段定义后再次运行！")
                exit(0)
            # s设置关联的字段
            in_link = input("请设置进行关联的字段（多个字段以逗号进行分割，建议设置可以表示用户唯一性的字段，已内置：%s）："%(','.join(links)))
            tamp_in_link = in_link.split(',')
            no_link = list(filter(lambda x: x not in header,tamp_in_link))
            if len(in_link) != 0 and len(no_link) > 0:
                print("输入的字段名有误，请确认后重新输入：" + ','.join(no_link))
                exit(0)
            links.extend(tamp_in_link)

            temp_datas = []
            header_len = len(header)
            # 临时存放不符合的数据
            temp_no_data_len_errs = []
            temp_no_data_val_errs = []
            # 先预加载一边数据，并进行数据校验判断
            for row in reader:
                if header_len != len(row):
                    temp_no_data_len_errs.append(row)
                for r in row:
                    if len(r) == 0:
                        temp_no_data_val_errs.append(row)
                        break
                temp_datas.append(row)

            # 检测到一下数据存在问题
            if len(temp_no_data_len_errs) > 0:
                print('以下行单元格数量和首行不同，请检查！')
                for temp_no_data_len_err in temp_no_data_len_errs:
                    print(temp_no_data_len_err)
            if len(temp_no_data_val_errs) > 0:
                print('以下行单元格内存在空值，请检查！')
                for temp_no_data_val_err in temp_no_data_val_errs:
                    print(temp_no_data_val_err)
            if len(temp_no_data_len_errs) > 0 or len(temp_no_data_val_errs) > 0 :
                exit(0)

            # 数据校验成功，给当前数据设定一个唯一名称值，方便后续进行处理
            project = input('请为当前数据集设定一个项目名称：')

            for row in temp_datas:
                cells = []
                # 解析当前行
                for index,item in enumerate(row):
                    if len(item) > 0:
                        cells.append({'name':header[index], 'val':item})
                save_data(cells,project)
    except Exception as e:
        print(e)
        if code_type=='utf-8-sig':
            print("变更文件加载格式为GB2312！")
            read_csv(csv_file_name,'GB2312')

# 读取txt文件方式
def read_txt(txt_file_name, code_type='utf-8-sig') :
    try:
        with open(txt_file_name, encoding=code_type) as f:
            header = f.readline().strip().split(",")
            print(header)
            # 该处加入手动输入确认
            yn = input("请确认字段定义行（Y or N）：")
            if yn != "Y" and yn != "y":
                print("请确认字段定义后再次运行！")
                exit(0)

            in_link = input("请设置进行关联的字段（多个字段以逗号进行分割，建议设置可以表示用户唯一性的字段，已内置：%s）："%(','.join(links)))
            
            tamp_in_link = in_link.split(',')
            no_link = list(filter(lambda x: x not in header,tamp_in_link))
            if (len(in_link) != 0 and len(no_link) > 0):
                # print(1111)
                print("输入的字段名有误，请确认后重新输入：" + ','.join(no_link))
                exit(0)
            links.extend(tamp_in_link)

            temp_datas = []

            header_len = len(header)
            # 临时存放不符合的数据
            temp_no_data_len_errs = []
            temp_no_data_val_errs = []
            # 先预加载一边数据，并进行数据校验判断
            for line in f:
                row = line.strip().split(',')
                if header_len != len(row):
                    temp_no_data_len_errs.append(row)
                for r in row:
                    if len(r) == 0:
                        temp_no_data_val_errs.append(row)
                        break
                temp_datas.append(row)

            # 检测到一下数据存在问题
            if len(temp_no_data_len_errs) > 0:
                print('以下行单元格数量和首行不同，请检查！')
                for temp_no_data_len_err in temp_no_data_len_errs:
                    print(temp_no_data_len_err)
            if len(temp_no_data_val_errs) > 0:
                print('以下行单元格内存在空值，请检查！')
                for temp_no_data_val_err in temp_no_data_val_errs:
                    print(temp_no_data_val_err)
            if len(temp_no_data_len_errs) > 0 or len(temp_no_data_val_errs) > 0 :
                exit(0)

            # 数据校验成功，给当前数据设定一个唯一名称值，方便后续进行处理
            project = input('请为当前数据集设定一个项目名称：')

            for row in temp_datas:
                cells = []
                # 解析当前行
                for index,item in enumerate(row):
                    if len(item) > 0:
                        cells.append({'name':header[index], 'val':item})
                save_data(cells,project)
    except Exception as e:
        print(e)

        if code_type=='utf-8-sig':
            print("变更文件加载格式为GB2312！") 
            read_txt(txt_file_name,'GB2312')


# 对数据进行存储
def save_data(cells,project):
    # try:
        if len(cells) <= 1:
            print("数据内容过少，跳过当前行")
            print(cells)
            return
        # 当前时间错
        ts = int(datetime.datetime.now().timestamp())

        # 查看当前条数据是否已有数据存在库中
        values = check_data_if_exists(cells)
        # 如果查询到的数据的长度 和 当前待插入的数据的长度 相同，则表示数据已经存在，不需要再进行插入
        if len(values) == len(cells):
            return

        # 存放已经存在的数据的集合
        temp_db_vals = list(map(lambda x: x['val'], values))
        # 数据待插入的uid集合
        new_uids = []
        # 如果长度为0 则表示都为新数据
        if len(values) == 0 :
            uid = str(uuid.uuid4()).replace("-","")
            new_uids.append(uid)
        # 否侧表示当前已有数据存在数据库中，获取这些数据中的uid，并进行去重
        # 只有设定的name才能进行关联
        else:            
            new_uids = list(set(map(lambda x:x['uuid'],values)))
        
        print(cells)
        conn = sqlite3.connect(db_file_name)
        cursor = conn.cursor()
        # 新数据每个关联的不同的uid都需要进行添加
        for new_uid in new_uids:
            # 对数据进行到库中
            for cell in cells:
                # 如果当前值不存在，则进行插入
                if cell['val'] not in temp_db_vals:
                    cursor.execute('INSERT INTO mswd (name,val,uuid,create_time,project) VALUES (?,?,?,?,?)',
                        (cell['name'],cell['val'], new_uid,ts,project))
                # 如果存在则在原来的数据的关联字段上面加上新的项目名称
                else:

                    exist_other_project = cursor.execute('select id from mswd where (uuid = ? and val = ? and project = ?) or (uuid = ? and val = ? and other_project like ? )',
                        (new_uid, cell['val'],project,new_uid, cell['val'],'%'+project+'%'))
                    exist_other_project = list(map(lambda x:x[0],exist_other_project))
                    if len(list(exist_other_project)) == 0:
                        cursor.execute("update mswd set other_project = other_project||? where uuid = ? and val = ?",
                                            (project + 'ddv',new_uid, cell['val']))

        conn.commit()
        conn.close()

        return

    # except Exception as e:
    #     print(e)
    #     exit(0)


# 判断待加载的文件是否存在
def file_exists(file_name):
    if os.path.exists(file_name):
        return True
    else:
        return False

def run():

    # 当前支持的文件格式
    types = {
        '.csv':read_csv,
        '.txt':read_txt,
    }

    print("当前支持的格式：" + ",".join(types))
    file_name = input("请输入待加载的文件名：")
    # 判断文件是否存在
    if not os.path.exists(file_name):
        print("该文件不存在")
        print("请正确输入文件名")
        exit(0)
    elif not os.path.isfile(file_name):
        print("该地址不是文件")
        print("请正确输入文件名")
        exit(0)

    # 判断文件格式是否符合
    file_end_name = os.path.splitext(file_name)[-1]

    if file_end_name not in list(types.keys()):
        print("当前文件格式暂不支持，请输入一下格式文件")
        print(','.join(types))
        exit(0)

    # 调用对应解析文件的函数
    types[file_end_name](file_name)


# 读取db内容
def read_db():

    print("------------------------------------------------")
    val = input("请输入查询条件：")
    if len(val) == 0:
        print("请输入有效的查询条件！")
        exit(0)
    print("查询条件：" + str(val))
    conn = sqlite3.connect(db_file_name)
    cursor = conn.cursor()
    values = cursor.execute('select * from mswd where val = ?', (val,))
    # print(res)
    result_values = list(map(lambda x:x,values))
    if len(result_values) == 0:
        print("没有查询到数据")
        exit(0)
    print("查询到数据：")

    index = 1
    for result_value in result_values:
        # result_value = result_values[0]
        name = result_value[1]
        val = result_value[2]
        uid = result_value[3]
        create_time = result_value[4]
        project = result_value[5]
            
        timeArray = time.localtime(create_time)
        create_time = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)

        tb = pt.PrettyTable()
        tb.field_names = ["field", "value", "create","project"]
        tb.add_row([name,val,create_time,project])

        values = cursor.execute('select * from mswd where uuid = ? and val != ?', (uid,val))
        values = list(map(lambda x:x,values))
        if len(values) > 0:
            print("查询到关联数据：")

            print("------------------------"+str(index)+"-----------------------")
        for value in values:
            name = value[1]
            val = value[2]
            uid = value[3]
            create_time = value[4]
            project = value[5]
            timeArray = time.localtime(create_time)
            create_time = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
            tb.add_row([name,val,create_time,project])

        print(tb)
        index += 1
    
    conn.commit()
    conn.close()


# 查询所有数据
def read_all():
    conn = sqlite3.connect(db_file_name)
    cursor = conn.cursor()
    # values = cursor.execute('select name from mswd where project = "sampletxt" group by name')
    cursor.execute("update mswd set other_project = other_project||? where uuid = ? and val = ?",
                        ('sampletxtaaaa1' + 'ddv','f3f7e9628c8744e798fabea145ca57581', '1811111111112'))
    # print(res)
    values = cursor.execute('select * from mswd')
    values = list(map(lambda x:x,values))
    for value in values:
        print(value)
    conn.commit()
    conn.close()


# 查询当前已有字段
def read_col():
    conn = sqlite3.connect(db_file_name)
    cursor = conn.cursor()
    values = cursor.execute('select name,create_time from mswd group by name')
    values = list(map(lambda x:x,values))
    
    tb = pt.PrettyTable()
    tb.field_names = ["field","create_time"]

    for value in values:
        timeArray = time.localtime(value[1])
        create_time = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
        tb.add_row([value[0],create_time])
    conn.commit()
    conn.close()
    print(tb)


# 查询当前所哟项目名
def read_project():
    conn = sqlite3.connect(db_file_name)
    cursor = conn.cursor()
    values = cursor.execute('select project,create_time from mswd group by project')

    tb = pt.PrettyTable()
    tb.field_names = ["project","create_time"]
    values = list(map(lambda x:x,values))
    for value in values:
        timeArray = time.localtime(value[1])
        create_time = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
        tb.add_row([value[0],create_time])

    conn.commit()
    conn.close()
    print(tb)


# 导出指定字段的数据
def export_data():
    print("进行数据导出")

    conn = sqlite3.connect(db_file_name)
    cursor = conn.cursor()
    names = cursor.execute('select name from mswd group by name')
    names = list(map(lambda x:x[0],names))
    
    input_names = input("请输入当前需要导出的字段（多个字段以逗号进行分割，第一个字段将会被作为主键。%s）" % (','.join(names)))
    # 检查输入的字段名称是否有效
    input_names = input_names.split(",")
    input_names = list(filter(lambda x: len(x) > 0 , input_names))
    no_names = list(filter(lambda x: x not in names,input_names))
    if len(no_names) > 0:
        print("以下字段名称不存在:" + ','.join(no_names))
        exit(0)

    # 选择需要导出的字段值，以第一个字段值作为主键
    # 搜索主键的有多少数据
    names = cursor.execute('select count(id) from mswd where name = ?',(input_names[0],))
    names = list(names)
    print("根据'%s'当前已收集到数量：%s"%(input_names[0],str(names[0][0])))
    export_number = input('请输入导出数量：')
    if not export_number.isdigit():
        print("请输入数字类型")
        exit()

    vals = cursor.execute('select name,val,uuid from mswd where name = ? limit ?',(input_names[0],export_number))
    vals = list(vals)
    if len(vals) > 0:

        create_file_time = time.strftime("%Y-%m-%d-%H-%M-%S")
        print("数据将写入:%s-%s.csv"%(input_names[0],create_file_time))

        with open('%s-%s.csv'%(input_names[0],str(create_file_time)), 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(input_names)
            print(input_names)
            for val in vals:
                datas = []
                data = {}
                uuid = val[2]
                number = 1
                # 根据这个uuid再去查询对应的其他数据
                uuid_datas = cursor.execute('select name,val from mswd where uuid = ?',(uuid,))
                for uuid_data in list(uuid_datas):
                    # 如果对应的name是要查询出来的就进行添加
                    if uuid_data[0] in input_names:
                        if uuid_data[0] in data:
                            data[uuid_data[0]].append(uuid_data[1])
                            if len(data[uuid_data[0]]) > number:
                                number += 1
                        else:
                            data[uuid_data[0]] = [uuid_data[1]]

                for i in range(number):
                    line = []
                    for input_name in input_names:
                    
                        if input_name not in data:
                            # print("%s:%s"%(input_name,""))
                            line.append("")
                        else:
                            if i < len(data[input_name]) - 1:
                                line.append(data[input_name][i])
                            else:
                                line.append(data[input_name][len(data[input_name]) - 1])

                    print(line)
                    writer.writerow(line)

    conn.commit()
    conn.close()


# 指定项目名称读取数据
def export_project():
    print("进行数据导出")
    # 显示当前字段

    conn = sqlite3.connect(db_file_name)
    cursor = conn.cursor()
    projects = cursor.execute('select project from mswd group by project')
    projects = list(map(lambda x:x[0],projects))
    
    input_project = input("请输入当前需要导出的项目（%s）" % (','.join(projects)))
    # 检查输入的字段名称是否有效
    if input_project not in projects:
        print("以下项目项目不存在:" + input_project)
        exit(0)

    project_names = cursor.execute('select name from mswd where project = ? group by name',(input_project,))
    project_names = list(map(lambda x:x[0],project_names))

    # 请设定要进行数据关联的字段
    input_project_name = input("请设定要进行数据的主键字段（当前项目字段包含：%s，默认状态为第一个字段）："%(",".join(project_names)))
    input_project_name_index = 0
    if input_project_name in project_names:
        input_project_name_index = project_names.index(input_project_name)

    create_file_time = time.strftime("%Y-%m-%d-%H-%M-%S")
    print("数据将写入:%s-%s.csv"%(input_project,create_file_time))
    name_vals = cursor.execute('select name,val,uuid from mswd where project = ?',(input_project,))
    name_vals = list(name_vals)

    datas = {}
    
    for name_val in name_vals:
        name = name_val[0]
        val = name_val[1]
        uuid = name_val[2]
        if uuid not in datas:
            datas[uuid] = [{name:val}]
        else:
            if name not in datas[uuid][0]:
                datas[uuid][0][name] = val
            else:
                # 第一个字段为关联字段
                if project_names[input_project_name_index] == name:
                    d = copy.deepcopy(datas[uuid][0])
                    d[name] = val
                    datas[uuid].append(d)

    with open('%s-%s.csv'%(input_project,str(create_file_time)), 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(project_names)
        print(project_names)
        for uuid in datas.keys():

            for data_array in datas[uuid]:
                # 按设定顺序进行输出
                line = []
                
                for project_name in project_names:
                    if project_name not in data_array:
                        # 如果当前name应该有的数据没有在当前情况内，那么冲ohter_project字段进行查询一次
                        other_project_val = cursor.execute('select val from mswd where uuid = ? and name = ? and other_project like ?',
                            (uuid,project_name,'%'+input_project+'%'))
                        other_project_val = list(map(lambda x:x[0],other_project_val))
                        if len(other_project_val) != 0:
                            line.append(other_project_val[0])
                        else:
                            line.append("")
                    else:
                        line.append(data_array[project_name])
                print(line)
                writer.writerow(line)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    ban = """
       __  __  _______          _______  
      |  \/  |/ ____\ \        / /  __ \ 
      | \  / | (___  \ \  /\  / /| |  | |
      | |\/| |\___ \  \ \/  \/ / | |  | |
      | |  | |____) |  \  /\  /  | |__| |
      |_|  |_|_____/    \/  \/   |_____/ 
    
                         v:1.0  author:ddv      
    """
    print(ban)
    option = input("""
请选择相应功能:
1、录入数据
2、查询数据
3、查看当前已有字段
4、查看当前已有项目名
5、指定字段名称导出数据
6、指定项目名称导出数据
""")
    if option not in ['1','2','3','4','5','6']:
        # read_all()
        print("请进行正确的选择！")
        exit(0)
    
    # 判断db文件数据已经存储，如不存在则创建
    init_db()

    if option == '1':
        run() 
    elif option == '2':
        read_db()
    elif option == '3':
        read_col()
    elif option == '4':
        read_project()
    elif option == '5':
        export_data()
    elif option == '6':
        export_project()
