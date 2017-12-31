
'''定时获取最新的数据   调用相应的模型  对数据进行分类'''
import pymysql
from sklearn.externals import joblib
import datetime
import numpy as np
import json
import math
import time

'''获取监测点的信息'''
# 打开数据库连接
db = pymysql.connect("localhost", "root", "123456", "geologicmessage")
# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()
point_list = []  # 监测点编号

'''字典 存放设备 属性索引'''
device_index_dict = {'convergence': 0, 'crack': 1, 'displacement': 2, 'hydraulic_pressure': 3,
                'moisture': 4, 'mud_depth': 5, 'osmotic_pressure': 6, 'rainfall': 7,
                'settlement': 8, 'soil_pressure': 9, 'temperature': 10, 'voltage': 11,
                'water_level': 12, 'gps': 13, 'tilt': 14, 'clinometer': 15}

'''多参数属性'''
double_device_type_list = ['tilt', 'wind']  # 两参数类型
third_device_type_list = ['clinometer']  # 三参数类型
mul_device_type_list = ['gps']  # 多参数类型
not_use_type_list = ['humidity', 'wind']  # 不考虑的类型

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

'''
新数据
调用已经训练好的模型 进行分类
此处可进行普通传值
'''
'''遍历监测点  根据监测点 寻找当前最新的数据   将所得的字典 转换为数组  去调用相应检测点的模型'''
def find_data(time_tamp):
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
    for _point in point_list:
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
                            cursor.execute(
                                calc_result(2, _point, point_device_id_row[0], _type, time_tamp, passageway_row))
                            double_type_results = cursor.fetchall()
                            #print(double_type_results)
                            if len(double_type_results) == 2:
                                double_result = (math.sqrt(
                                    double_type_results[0][0] ** 2 + double_type_results[0][1] ** 2) - math.sqrt(
                                    double_type_results[1][0] ** 2 + double_type_results[1][1] ** 2)) / ((time.mktime(
                                    double_type_results[0][2].timetuple()) - time.mktime(
                                    double_type_results[1][2].timetuple())) / 3600)
                                # print(double_result)
                                if _type not in device_data_dict.keys():
                                    device_data_dict[_type] = double_result
                                if _type in device_data_dict.keys() and math.fabs(device_data_dict[_type]) < math.fabs(
                                        double_result):
                                    device_data_dict[_type] = double_result

                    elif _type in third_device_type_list:  # 判断为三参数类型
                        for passageway_row in passageway_id_list:
                            cursor.execute(
                                calc_result(3, _point, point_device_id_row[0], _type, time_tamp, passageway_row))
                            third_type_results = cursor.fetchall()
                            if len(third_type_results) == 2:
                                #(third_type_results)
                                third_result_list = []
                                for number in range(3):
                                    third_result_list[number] = (third_type_results[0][number] - third_type_results[1][
                                        number]) / ((time.mktime(third_type_results[0][3].timetuple()) - time.mktime(
                                        third_type_results[1][3].timetuple())) / 3600)
                                for max_third_result in third_result_list:
                                    device_data_dict[_type] = third_result_list[0]
                                    if math.fabs(device_data_dict[_type]) < math.fabs(max_third_result):
                                        device_data_dict[_type] = max_third_result

                    elif _type in mul_device_type_list:  # 判断为多参数属性  GNSS只考虑后三个参数
                        for passageway_row in passageway_id_list:
                            cursor.execute(
                                calc_result(9, _point, point_device_id_row[0], _type, time_tamp, passageway_row))
                            mul_type_results = cursor.fetchall()
                            if len(mul_type_results) == 2:
                                mul_result = (math.sqrt(
                                    mul_type_results[0][6] ** 2 + mul_type_results[0][7] ** 2 + mul_type_results[0][
                                        8] ** 2) - math.sqrt(
                                    mul_type_results[1][6] ** 2 + mul_type_results[1][7] ** 2 + mul_type_results[1][
                                        8] ** 2)) / ((time.mktime(mul_type_results[0][9].timetuple()) - time.mktime(
                                    mul_type_results[1][9].timetuple())) / 3600)
                                if _type not in device_data_dict.keys():
                                    device_data_dict[_type] = mul_result
                                if _type in device_data_dict.keys() and math.fabs(device_data_dict[_type]) < math.fabs(
                                        mul_result):
                                    device_data_dict[_type] = mul_result
                    else:
                        for passageway_row in passageway_id_list:
                            #print(passageway_row)
                            cursor.execute(
                                calc_result(1, _point, point_device_id_row[0], _type, time_tamp, passageway_row))
                            single_type_results = cursor.fetchall()
                            if len(single_type_results) == 2:
                                single_result = (single_type_results[0][0] - single_type_results[1][0]) / ((time.mktime(
                                    single_type_results[0][1].timetuple()) - time.mktime(
                                    single_type_results[1][1].timetuple())) / 3600)
                                # print(single_result)
                                if _type not in device_data_dict.keys():
                                    device_data_dict[_type] = single_result
                                if _type in device_data_dict.keys() and math.fabs(device_data_dict[_type]) < math.fabs(
                                        single_result):
                                    device_data_dict[_type] = single_result

        except:
            print("Error: unable to fetch data")
        device_num_dict = {} #用来存储编号 和 变化率
        device_num_list=[]   #用来存储调用模型的数据 （有顺序的）
        flag = 0  # 标志是否有异常数据
        '''判断字典中是否有值  是否有异常值  若有异常值  不进行插入操作  空值以0补缺'''
        for point_device_row in point_device_list:
            if point_device_row not in device_data_dict.keys():
                device_data_dict[point_device_row] = 0
            else:
                if math.fabs(device_data_dict[point_device_row]) > 1:
                    flag = 1
                    break

        try:
            if flag == 0:
                '''遍历device_data_dict 根据device_data_dict的key获取device_index_dict中的index 保存到device_num_dict'''
                for key, value in device_data_dict.items():
                    if key in device_index_dict:
                        device_num_dict[str(device_index_dict[key])] = device_data_dict[key]

                '''从一到十七 遍历 寻找device_num_dict 将变化率 存入device_num_list'''
                for num in range(17):
                    if str(num) in device_num_dict:
                        device_num_list.append(device_num_dict[str(num)])
                # x_test1 = np.array([['1.2', '4.2', '3', '5.7', '12']])
                # print(type(x_test1))
                x_test = np.array(device_num_list)
                clf = joblib.load(_point + "train_model.m")
                y_predict = clf.predict(x_test.reshape(1, -1))
                print(_point)
                print(y_predict)
            else:
                # 数据有异常 不调用模型  给系统返回错误码
                print("error!")
        except Exception as e:
            print(e)

'''调用分类方法'''
time_tamp = 1512136800
try:
    find_data(time_tamp)
except Exception as e:
    print(e)
