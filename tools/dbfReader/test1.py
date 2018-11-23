#!/usr/bin/python
#-*-coding:utf-8-*-  

"""
把指定目录下的所有dbf文件转为csv文件，合并csv文件并处理筛选需要的结果
author:caoyongzhi
"""
import os
from collections import OrderedDict
import dbfReader
import pandas as pd
import traceback

FILE_PATH_READ = r'C:\Users\cao62\Desktop\dbfReader'
FILE_PATH_SAVE = r''
FILE_TYPE_DBF = '.dbf'
FILE_TYPE_CSV = '.csv'

#---------------------------------------------------------------------
# 获得指定目录下所有DBF文件
def get_dbf_files(file_path):
    """"""
    file_dict = OrderedDict()      # dbf文件字典
    
    for root, dirs, files in os.walk(file_path):
        for f in files:
            file_name = os.path.splitext(f)[0]
            file_type = os.path.splitext(f)[1]
            if file_type == FILE_TYPE_DBF:
                file_dict[file_name] = os.path.join(root, f)
    return file_dict

#---------------------------------------------------------------------
# 合并所有csv文件，删除原csv文件，并返回dataframe格式文件
def merge_files(file_dict):
    """"""
    data_df = pd.DataFrame()
    i = 0
    for k, d in file_dict.items():
        if i == 0:
            data_df = pd.DataFrame.from_csv(d)
        else:
            data_df = pd.concat([data_df, pd.DataFrame.from_csv(d)], axis=0)
            
        i += 1
        os.remove(d)             # 删除文件
    
    
    return data_df       

#---------------------------------------------------------------------
# 对数据进行加工处理，并输出csv格式的结果
def data_manager(data_df):
    """"""
    print data_df

#---------------------------------------------------------------------
if __name__ == '__main__':
    
    print u'开始获取dbf文件'
        
    dbf_file_dict = get_dbf_files(FILE_PATH_READ)         # 获取指定文件夹下的所有文件名字和路径
    csv_file_dict = OrderedDict()                         # 转换后csv文件字典
    
    for k, d in dbf_file_dict.items():
        try:
            dbf = dbfReader.DBF(d)
            dbf.to_csv(k+FILE_TYPE_CSV)
            csv_file_dict[k] = os.path.splitext(d)[0] + FILE_TYPE_CSV
        except:
            traceback.print_exc()
            
    print u'开始合并数据'
    data_df = merge_files(csv_file_dict)
    
    data_manager(data_df)
    
    print u'数据处理完成'