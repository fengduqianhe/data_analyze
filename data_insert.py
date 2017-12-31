'''定时同步数据'''
import time
import pymysql
import datetime
import uuid
import math
import json

'''获取监测点的信息'''
# 打开数据库连接
db = pymysql.connect("localhost", "root", "123456", "geologicmessage")
# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()
point_list = []  # 监测点编号


'''查询所有的监测点编号'''
def get_point_list():
    sql = "select id from `point_message` ;"
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        point_results = cursor.fetchall()
        if len(point_results) > 0:
            for point_row in point_results:
                point_list.append(point_row[0])
    except:
        print("Error: unable to fetch data")
    return point_list

def get_point_device_list(_point):
    device_type_sql = "select device_type from `device` WHERE monitoring_area='%s'" % _point
    point_device_list = []  # 监测点对应设备类型
    try:
        # 执行SQL语句
        cursor.execute(device_type_sql)
        # 获取所有记录列表
        point_device_type_results = cursor.fetchall()
        for point_device_type_row in point_device_type_results:
            for _type in point_device_type_row[0].split(","):
                if _type not in point_device_list and _type not in not_use_type_list:
                    point_device_list.append(_type)
    # print(point_device_list)
    except:
        print("Error: unable to fetch data")
    return point_device_list


'''多参数属性'''
double_device_type_list = ['tilt', 'wind']  # 两参数类型
third_device_type_list = ['clinometer']  # 三参数类型
mul_device_type_list = ['gps']  # 多参数类型
not_use_type_list = ['humidity', 'wind']  #不考虑的类型


'''拼接查询语句'''
'''num参数个数 1,2,3,9'''
'''point 监测点编号'''
'''device_id 设备id'''
'''dp 设备类型'''
'''time_tamp 时间戳'''
'''passagewaySum 通道数'''
def calc_result(num, point, device_id, dp, time_tamp, passagewaySum):
    para = ""  # 查询参数
    month = str(datetime.datetime.now().month) if datetime.datetime.now().month > 10 else "0" + str(
        datetime.datetime.now().month)
    device_data_name = "device_data_" + dp + "_" + str(datetime.datetime.now().year) + month
    for _p in range(num):
        para = para + "para" + str(_p + 1) + ","
    para = para[:-1]
    # print(device_id)
    # print(passageway)
    calc_data_sql = "SELECT " + para + ", coltime  FROM " + device_data_name + " WHERE unix_timestamp(coltime)<" + \
                    str(time_tamp) +" AND monitoring_area = '%s'" % point + "AND device_id = '%s'" % device_id +\
                    "AND passageway  = '%s'" % passagewaySum + "ORDER BY coltime DESC LIMIT 2"  # 查询距当前时间最近的两组数据
    # print(calc_data_sql)
    return calc_data_sql

# 计算device_data_dict字典
def get_device_data_dict(_point):
    sql = "select id,device_type,passageway_place from `device` WHERE monitoring_area='%s'" % _point
    device_data_dict = {}  # 监测点对应各设备数据
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        point_device_id_results = cursor.fetchall()
        for point_device_id_row in point_device_id_results:  # 遍历设备 获取设备的属性  通道
            passageway_json = point_device_id_row[2]  # 记录通道json值   解析
            '''解析passageway_place 的json数据'''
            passageway_data = json.loads(passageway_json)

            '''遍历该设备的设备类型  获取该设备类型的通道数'''
            for _type in point_device_id_row[1].split(","):
                '''获取到类型 查找通道'''
                passageway_id_list = []  # 记录通道的组合
                for passageway_num in range(len(passageway_data)):
                    if passageway_data[passageway_num]['deviceType'] == _type:
                        passageway_id_list.append(passageway_data[passageway_num]['id'])

                if _type in double_device_type_list:  # 判断为两参数类型
                    for passageway_row in passageway_id_list:
                        cursor.execute(calc_result(2, _point, point_device_id_row[0], _type, time_tamp, passageway_row))
                        double_type_results = cursor.fetchall()
                        print(double_type_results)
                        if len(double_type_results) == 2:
                            double_result = (math.sqrt(double_type_results[0][0] ** 2 + double_type_results[0][1] ** 2) - math.sqrt(double_type_results[1][0] ** 2 + double_type_results[1][1] ** 2)) / ((time.mktime(double_type_results[0][2].timetuple()) - time.mktime(double_type_results[1][2].timetuple())) / 3600)
                            # print(double_result)
                            if _type not in device_data_dict.keys():
                                device_data_dict[_type] = double_result
                            if _type in device_data_dict.keys() and math.fabs(device_data_dict[_type]) < math.fabs(
                                    double_result):
                                device_data_dict[_type] = double_result

                elif _type in third_device_type_list:  # 判断为三参数类型
                    for passageway_row in passageway_id_list:
                        cursor.execute(calc_result(3, _point, point_device_id_row[0], _type, time_tamp, passageway_row))
                        third_type_results = cursor.fetchall()
                        if len(third_type_results) == 2:
                            print(third_type_results)
                            third_result_list = []
                            for number in range(3):
                                third_result_list[number] = (third_type_results[0][number] - third_type_results[1][number]) / ((time.mktime(third_type_results[0][3].timetuple()) - time.mktime(third_type_results[1][3].timetuple())) / 3600)
                            for max_third_result in third_result_list:
                                device_data_dict[_type] = third_result_list[0]
                                if math.fabs(device_data_dict[_type]) < math.fabs(max_third_result):
                                    device_data_dict[_type] = max_third_result

                elif _type in mul_device_type_list:  # 判断为多参数属性  GNSS只考虑后三个参数
                    for passageway_row in passageway_id_list:
                        cursor.execute(calc_result(9, _point, point_device_id_row[0], _type, time_tamp, passageway_row))
                        mul_type_results = cursor.fetchall()
                        if len(mul_type_results) == 2:
                            mul_result = (math.sqrt(mul_type_results[0][6] ** 2 + mul_type_results[0][7] ** 2 + mul_type_results[0][8] ** 2) - math.sqrt(mul_type_results[1][6] ** 2 + mul_type_results[1][7] ** 2 + mul_type_results[1][8] ** 2)) / ((time.mktime(mul_type_results[0][9].timetuple()) - time.mktime(mul_type_results[1][9].timetuple())) / 3600)
                            if _type not in device_data_dict.keys():
                                device_data_dict[_type] = mul_result
                            if _type in device_data_dict.keys() and math.fabs(device_data_dict[_type]) < math.fabs(
                                    mul_result):
                                device_data_dict[_type] = mul_result
                else:
                    for passageway_row in passageway_id_list:
                        print(passageway_row)
                        cursor.execute(calc_result(1, _point, point_device_id_row[0], _type, time_tamp, passageway_row))
                        single_type_results = cursor.fetchall()
                        if len(single_type_results) == 2:
                            single_result = (single_type_results[0][0] - single_type_results[1][0]) / ((time.mktime(single_type_results[0][1].timetuple()) - time.mktime(single_type_results[1][1].timetuple())) / 3600)
                            # print(single_result)
                            if _type not in device_data_dict.keys():
                                device_data_dict[_type] = single_result
                            if _type in device_data_dict.keys() and math.fabs(device_data_dict[_type]) < math.fabs(
                                    single_result):
                                device_data_dict[_type] = single_result

    except:
        print("Error: unable to fetch data")
    return device_data_dict

'''数据插入'''
'''考虑通道数后对变化量求出最大值'''
'''根据监测点 寻找设备 并记录设备的id'''
'''遍历设备  记录该设备拥有的设备类型 并获取通道数 根据对应监测点ID 设备id 通道数 计算变化率 求出最大值    '''
def insert_data(time_tamp):
    point_list = get_point_list()
    for _point in point_list:
        device_data_dict = get_device_data_dict(_point)
        '''获取监测点设备列表'''
        point_device_list = get_point_device_list(_point)
        flag = 0  # 标志是否有异常数据

        '''判断字典中是否有值  是否有异常值  若有异常值  不进行插入操作  空值以0补缺'''
        for point_device_row in point_device_list:
            if point_device_row not in device_data_dict.keys():
                device_data_dict[point_device_row] = 0
            else:
                if math.fabs(device_data_dict[point_device_row]) > 1:
                    flag = 1
                    break

        if flag == 0:
            '''监测点--设备插入历史表'''
            uu_id = str(uuid.uuid1())  # 产生uuid
            insert_name_sql = "INSERT INTO device_history_data(id,monitoring_area,result) VALUES ('%s','%s','%s')" % (
                uu_id, _point, '0')
            try:
                cursor.execute(insert_name_sql)
                db.commit()
            except:
                db.rollback()  # 发生错误时回滚

                '''设备数据入表'''
            for key, value in device_data_dict.items():
                update_data_sql = "UPDATE device_history_data SET " + key + " ='%s' WHERE monitoring_area='%s'AND id='%s'" % (
                    value, _point, uu_id)
                print(update_data_sql)
                try:
                    cursor.execute(update_data_sql)
                    db.commit()
                except:
                    db.rollback()  # 发生错误时回滚
            '''重新设置时间'''
            update_time_sql = "UPDATE device_history_data SET coltime = FROM_UNIXTIME('%s') WHERE monitoring_area='%s' AND id='%s'" % (
                str(time_tamp), _point, uu_id)
            print(update_time_sql)
            try:
                cursor.execute(update_time_sql)
                db.commit()
            except:
                db.rollback()  # 发生错误时回滚
    return


'''更新时间戳更改当前时间循环调用'''
time_tamp = 1512136800
while (time_tamp < 1514556801):
    insert_data(time_tamp)
    time_tamp+=64800



