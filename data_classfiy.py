
'''定时获取最新的数据   调用相应的模型  对数据进行分类'''
import pymysql
import numpy as np
from sklearn import neighbors
from sklearn.metrics import precision_recall_curve
from sklearn.metrics import classification_report
from sklearn.cross_validation import train_test_split
from sklearn.preprocessing import  StandardScaler
import matplotlib.pyplot as plt
from sklearn.externals import joblib
import datetime
import uuid

'''获取监测点的信息'''
# 打开数据库连接
db = pymysql.connect("localhost", "root", "123456", "geologicmessage")
# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()
point_list = []  # 监测点编号

'''查询所有的监测点编号'''
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

'''多参数属性'''
double_device_type_list = ['humidity', 'tilt', 'wind']  # 两参数类型
third_device_type_list = ['clinometer']  # 三参数类型
mul_device_type_list = ['gps']  # 多参数类型

'''拼接查询语句'''
'''num参数个数 1,2,3,9'''
'''point 监测点编号'''
'''dp 设备类型'''
'''time_tamp 时间戳'''
def connect_sql_para(num,point,dp,time_tamp):

    para = ""  #查询参数
    month=str(datetime.datetime.now().month) if datetime.datetime.now().month > 10 else "0" + str(datetime.datetime.now().month)
    device_data_name = "device_data_" + dp+"_"+ str(datetime.datetime.now().year) + month
    for _p in range(num):
        para = para+"para"+str(_p+1)+","
    para = para[:-1]
    '''查出最新的记录'''
    return "SELECT "+para +" FROM "+device_data_name+" WHERE coltime ="+"(SELECT max(coltime) FROM "+device_data_name+" WHERE monitoring_area = '%s')"% point+"AND monitoring_area = '%s'"% point


'''数据插入'''
'''time_tamp 时间戳'''
def find_data(time_tamp):

    '''根据监测点 查找该监测点所拥有的设备'''
    for _point in point_list:
        sql = "select device_type from `device` WHERE monitoring_area='%s'" % _point
        point_device_list = [] #监测点对应设备类型
        try:
            # 执行SQL语句
            cursor.execute(sql)
            # 获取所有记录列表
            point_device_type_results = cursor.fetchall()
            for point_device_type_row in point_device_type_results:
                for _type in point_device_type_row[0].split(","):
                    if _type not in point_device_list:
                        point_device_list.append(_type)
        except:
            print("Error: unable to fetch data")
        print(point_device_list)

        device_data_list = [] #监测点对应各设备数据
        '''根据监测点对应的设备 寻找最新的设备数据'''
        try:
            for dp in point_device_list:
                if dp in double_device_type_list:
                #判断为两参数类型
                    print(connect_sql_para(2, _point, dp, time_tamp))
                    cursor.execute(connect_sql_para(2, _point, dp, time_tamp))
                    double_type_results = cursor.fetchall()
                    if len(double_type_results) > 0:
                        print((double_type_results[0][0]+double_type_results[0][1])/2)
                        device_data_list.append((double_type_results[0][0]+double_type_results[0][1])/2)

                elif dp in third_device_type_list:          #判断为三参数类型
                   # print(connect_sql_para(3, _point, dp, time_tamp))
                    cursor.execute(connect_sql_para(3, _point, dp, time_tamp))
                    third_type_results = cursor.fetchall()
                    if len(third_type_results) > 0:
                        device_data_list.append((third_type_results[0][0]+third_type_results[0][1]+third_type_results[0][2])/3)
                elif dp in mul_device_type_list:
                    # print(connect_sql_para(9, _point, dp, time_tamp))
                    cursor.execute(connect_sql_para(9, _point, dp, time_tamp))
                    mul_type_results = cursor.fetchall()
                    if len(mul_type_results) > 0:
                        mul_sum = 0
                        for mul_i in range(9):
                            mul_sum += mul_type_results[0][mul_i]
                            device_data_list.append(mul_sum/9)

                else:                                       #判断为单参数类型
                    #print(connect_sql_para(1, _point, dp, time_tamp))
                    cursor.execute(connect_sql_para(1, _point, dp, time_tamp))
                    signal_type_results = cursor.fetchall()
                    if len(signal_type_results) > 0:
                        device_data_list.append(signal_type_results[0][0])
        except:
            print("Error: unable to fetch data")

        print(device_data_list)
        clf = joblib.load(_point+"train_model.m")
        y_predict = clf.predict(device_data_list)
        print(y_predict)

'''更新时间戳更改当前时间循环调用'''
time_tamp = 1512057600
while (time_tamp < 1514476800):
    find_data(time_tamp)
    time_tamp+=64800

    '''
    新数据
    调用已经训练好的模型 进行分类
    此处可进行普通传值


    import numpy as np
    from sklearn import neighbors
    from sklearn.externals import joblib
    x_test=np.array([['1.2','4.2','3','5.7'],['0.2','1.4','3','4.9']])
    clf = joblib.load("train_model.m")
    y_predict = clf.predict(x_test)
    print(y_predict)
    '''