'''
数据库中获取数据
进行KNN分析 调用sklearn库 进行分类
针对历史数据
'''
import pymysql
import numpy as np
from sklearn import neighbors
from sklearn.cross_validation import train_test_split
from sklearn.externals import joblib

# 打开数据库连接
db = pymysql.connect("localhost", "root", "123456", "geologicmessage")
# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()
# SQL 查询语句

'''根据监测点查找该监测点所拥有的设备'''
'''依次将监测点的设备数据存入数组中'''
'''查询所有的监测点编号'''
sql = "select id from `point_message` ;"
point_list = []
try:
    # 执行SQL语句
    cursor.execute(sql)
    # 获取所有记录列表
    point_results = cursor.fetchall()
    for point_row in point_results:
        point_list.append(point_row[0])
except:
    print("Error: unable to fetch data")

for _point in point_list:
    sql = "select device_type from `device` WHERE monitoring_area='%s'" % _point
    point_device_list = []  # 监测点对应的设备
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        point_device_results = cursor.fetchall()
        for point_device_row in point_device_results:
            for _type in point_device_row[0].split(","):
                if _type not in point_device_list:
                    point_device_list.append(_type)
    except:
        print("Error: unable to fetch data")

    '''查询当前月份所属季度的该监测点拥有设备的所有数据'''
    if len(point_device_list) == 0:
        continue
    para = ','.join(point_device_list)+",result"
    sql = "SELECT " + para + " FROM  device_history_data where QUARTER(coltime)=QUARTER(now()) AND monitoring_area='%s'" % _point
    # print(sql)
    data_list = []
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        results = cursor.fetchall()
        if len(results) == 0:
            continue
        data_list = np.array(results)

    except:
        print("Error: unable to fetch data")

    x = data_list[:, 0:data_list.shape[1]-1]
    labels = data_list[:, data_list.shape[1]-1]
    y = np.zeros(labels.shape)
    y[labels == '0'] = 0
    print(x)
    print(labels)

    '''拆分训练数据与测试数据 '''
    try:
        x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.25, random_state=33)
        ''' 训练KNN分类器(默认K=5) '''
        clf = neighbors.KNeighborsClassifier(algorithm='kd_tree')
        clf.fit(x_train, y_train)
        '''测试结果的打印'''
        y_predict = clf.predict(x_test)
        joblib.dump(clf, _point + "train_model.m")
        accurancy = np.mean(y_predict == y_test)
        print(accurancy)
        print(clf.score(x_test, y_test))
    except Exception as e:
        print("knn error")
        print(e)


