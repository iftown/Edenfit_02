# -*- coding: utf-8 -*-
"""
Created on Fri Sep 29 16:55:02 2017

@author: Administrator
"""


import os,datetime
import ctypes
import http
import pymysql
import tushare


#++++链接库并实现订阅的参数
def link_sub():
    dll = ctypes.CDLL('dll_sdk\\MateSDK.dll')  #导入行情接口DLL
    con = dll.ConnectServer(b'tcp://127.0.0.1:19908')  #实时行情地址
    sub = dll.Subscribe()  #订阅种类全部行情（）
    
    return dll,con,sub

#++++请求深度行情的参数
def http_get(StockCodeList):
    #print(StockCodeList)
    RQL = '/requestl2?list=' + StockCodeList 
    httpClient = None
    httpClient = http.client.HTTPConnection('localhost', 10010, timeout=30)
    httpClient.request('GET', RQL)
    
    response = httpClient.getresponse()
    
    return response

#+++++数据库参数
def db_con():
    conn=pymysql.connect(host='localhost',user='zdh_data',passwd='4WG5yzby85',db='zdh_data',port=3306)  #链接数据库  
    conn.set_charset('gbk')
    cur=conn.cursor() 
    
    return conn,cur

#++++++每天生成目录并保存当天更新的品种池列表文件
def mk_list():
    sz50 = tushare.get_sz50s()
    hs300 = tushare.get_hs300s()
    
    y = datetime.datetime.now().year
    m = datetime.datetime.now().month
    d = datetime.datetime.now().day
    path ='StockCodeList\\' + str(y) + '_' + str(m) + '_' + str(d) #本地日期作为目录名
    isExists=os.path.exists(path)
   
    if not isExists: #如果该目录不存在，创建目录并保存数据
        os.makedirs(path) 
        
    stocks = sz50,hs300
    for i in stocks:
        stock_list = []
        #print(i)
        for j in i.code:
            if (j[0] == '0') or (j[0] == '3'):
                j = 'SZ' + j
            elif j[0] == '6':
                j = 'SH' + j
            stock_list.append(j)
        #保存当天获取的sz50证券池代码用于核对
        fss = path +'\\' + 'list' + str(len(i.code)) + '.txt'
        f = open(fss,'w')
        for j in stock_list:
            f.write(j)
            f.write('\n')
        f.close()
    

#++++++++将品种目录拼接到一个对象并去重++++++++++++++++++++++++++
def union_list():
    y = datetime.datetime.now().year
    m = datetime.datetime.now().month
    d = datetime.datetime.now().day
    path ='StockCodeList\\' + str(y) + '_' + str(m) + '_' + str(d) #本地日期
    
    StockCodePaper = []
    f = open('StockCodeList\\IndexList.txt','r')
    lines = f.readlines()
    for line in lines:
        #global StockCodePaper
        line = line.strip('\n')
        StockCodePaper.append(line)

    for root, dirs, files in os.walk(path): #遍历指定路径下的所有文件
       for fs in files:
           fss = path + '\\' + fs
           f = open(fss,'r')
           lines = f.readlines()
           for line in lines:
               line = line.strip('\n')
               StockCodePaper.append(line)

    StockCodePaper = list(set(StockCodePaper)) #列表去重
    StockCodeList = ','.join(StockCodePaper) #将合并后的代码LIST赋值给字符串
    f.close()
    
    return StockCodePaper,StockCodeList      

    
   
#mk_list()            
#union_list()  
#aa,bb = union_list()
#http_get(bb)     





