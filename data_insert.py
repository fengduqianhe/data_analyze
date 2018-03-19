'''定时同步数据'''
from threading import Timer
import time
import pymysql
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
    #return "SELECT "+para +" FROM "+device_data_name+" WHERE coltime ="+"(SELECT max(coltime) FROM "+device_data_name+" WHERE monitoring_area = '%s')"% point+"AND monitoring_area = '%s'"% point

    '''按当前时间 距离当前时间最近'''
    #return "SELECT " + para + " FROM " + device_data_name + " WHERE coltime =" + "(SELECT max(coltime) FROM " + device_data_name + " WHERE unix_timestamp(NOW())>unix_timestamp(coltime) AND monitoring_area = '%s')" % point + "AND monitoring_area = '%s'" % point

    '''按指定时间戳 距离时间戳最近'''
    return "SELECT " + para + " FROM " + device_data_name + " WHERE coltime =" + "(SELECT min(coltime) FROM " + device_data_name + " WHERE '%s'<unix_timestamp(coltime) AND monitoring_area = '%s')" % (str(time_tamp),point) + "AND monitoring_area = '%s'" % point


'''数据插入'''
'''time_tamp 时间戳'''
def insert_data(time_tamp):

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

        device_data_dict = {} #监测点对应各设备数据
        '''根据监测点对应的设备 寻找最新的设备数据'''
        try:
            for dp in point_device_list:
                if dp in double_device_type_list:          #判断为两参数类型
                    print(connect_sql_para(2, _point, dp, time_tamp))
                    cursor.execute(connect_sql_para(2, _point, dp, time_tamp))
                    double_type_results = cursor.fetchall()
                    if len(double_type_results) > 0:
                        print((double_type_results[0][0]+double_type_results[0][1])/2)
                        device_data_dict.setdefault(dp, (double_type_results[0][0]+double_type_results[0][1])/2)

                elif dp in third_device_type_list:          #判断为三参数类型
                   # print(connect_sql_para(3, _point, dp, time_tamp))
                    cursor.execute(connect_sql_para(3, _point, dp, time_tamp))
                    third_type_results = cursor.fetchall()
                    if len(third_type_results) > 0:
                        device_data_dict.setdefault(dp,(third_type_results[0][0]+third_type_results[0][1]+third_type_results[0][2])/3)
                elif dp in mul_device_type_list:
                    # print(connect_sql_para(9, _point, dp, time_tamp))
                    cursor.execute(connect_sql_para(9, _point, dp, time_tamp))
                    mul_type_results = cursor.fetchall()
                    if len(mul_type_results) > 0:
                        mul_sum = 0
                        for mul_i in range(9):
                            mul_sum += mul_type_results[0][mul_i]
                        device_data_dict.setdefault(dp, mul_sum/9)

                else:                                       #判断为单参数类型
                    #print(connect_sql_para(1, _point, dp, time_tamp))
                    cursor.execute(connect_sql_para(1, _point, dp, time_tamp))
                    signal_type_results = cursor.fetchall()
                    if len(signal_type_results) > 0:
                        device_data_dict.setdefault(dp, signal_type_results[0][0])
        except:
            print("Error: unable to fetch data")

        '''监测点--设备插入历史表'''
        uu_id = str(uuid.uuid1())    #产生uuid
        insert_name_sql = "INSERT INTO device_history_data(id,monitoring_area,result) VALUES ('%s','%s','%s')" % (uu_id,_point, '0')
        try:
            cursor.execute(insert_name_sql)
            db.commit()
        except:
            db.rollback() # 发生错误时回滚

            '''设备数据入表'''
        for key, value in device_data_dict.items():
            update_data_sql = "UPDATE device_history_data SET " + key + " ='%s' WHERE monitoring_area='%s'AND id='%s'" % (value, _point, uu_id)
            print(update_data_sql)
            try:
                cursor.execute(update_data_sql)
                db.commit()
            except:
                db.rollback() # 发生错误时回滚
    return


'''更新时间戳更改当前时间循环调用'''
time_tamp = 1512057600
while (time_tamp < 1514476800):
    insert_data(time_tamp)
    time_tamp+=64800



