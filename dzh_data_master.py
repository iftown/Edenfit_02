# -*- coding: utf-8 -*-
"""
Created on Wed Sep 27 21:28:30 2017

@author: Administrator
"""
from queue import Queue
import ctypes
import json
import threading   #多线程模块
import time
import config as cf
import sys
from sqlalchemy import create_engine 
import pandas as pd
import datetime
#import make_data as md




#-----接收JSON串的线程---------------
class Producer(threading.Thread):
    
    def __init__(self, t_name, queue):
        threading.Thread.__init__(self,name=t_name)
        self.data=queue
    def run(self):
        
        sys.setrecursionlimit(1000000) #设迭代深度为一百万
        print('Edenfit_system is cennecting ... ')

        cf.mk_list()#建立当天的目录并生成品种池列表文件
        StockCodePaper,StockCodeList= cf.union_list() #以list和str两种方式返回当天更新的代码表
        
        dll,con,sub = cf.link_sub() #链接并订阅行情
        #conn,cur = cf.db_con() #首先连接数据库
        conn = create_engine('mysql+pymysql://dzh_data:wilrid@localhost:3306/dzh_data?charset=utf8')
        response = cf.http_get(StockCodeList)#请求深度行情
        re_reason = response.reason
        print('ConnectServer:',con) #返回0表示正常
        print('Subscribe:',sub)  #返回0表示正常
        print('http_GEt_status:',response.reason) #返回OK表示正常
        print(time.strftime('%Y-%m-%d %X',time.localtime()))
    
        if (con != 0) or (sub !=0 ) or (response.reason != 'OK'):
            print('请注意!!!ConnectServer:' + str(con) +';Subscribe:'+ str(sub) +';http_GEt_status:' + re_reason)
            print(time.strftime('%Y-%m-%d %X',time.localtime()))
        else:
            pass
        
        while True:  #开始接受JSON串
            try:
                db = ctypes.create_string_buffer(1024*1024*10) #初始化一个固定内存容量的变量
                data = dll.GetData(db,1024 * 1024 *10)  #db用来接收数据串，第二个参数是空间大小
                data_str = ctypes.string_at(db, -1).decode('gbk')  #取得C对象的值并转码
                DJ = json.loads(data_str)
                if (DJ['QuoteType'] == 'WDPK') or (DJ['QuoteType'] == 'SDPK'): #符合条件的JOSON才传入队列
                    #print(DJ)
                    pro_list = [conn,DJ,StockCodePaper,StockCodeList,dll,con,sub,re_reason]  #将要传入队列的数据封装成一个list变量
                    #print(pro_list)
                    self.data.put(pro_list) #将数据依次存入队列
            
            except:   #报异常
                print('JSON获取异常！！！')
                print((time.ctime()))
                break
        
        print ("%s: %s 数据生产结束!" %(time.ctime(), self.getName()))


#-----------行情订阅线程--------------------------------
class Http_get(threading.Thread):
    def __init__(self,t_name,queue):
        threading.Thread.__init__(self,name=t_name)
        self.data=queue
    
    def run(self):
        while True:
            try:
                link_status = self.data.get(1,30)
                StockCodeList = link_status[4]
                #dll = response[5]
                con = link_status[5]
                sub = link_status[6]
                http_status = link_status[7]
                if (con != 0) or (sub != 0):
                    print('注意！！行情链接出现问题！！！ con: %d , sub: %d !'%(con,sub))
                    break
                elif http_status != 'OK':
                    print('行情请求出现问题！！！ http_get_statu: %s' %http_status)
                    response = cf.http_get(StockCodeList)#请求深度行情
                else:
                    self.data.put(link_status)
                    time.sleep(1)
            
            except:   #等待输入，超过30秒就报异常
                print( "长时间没有收到状态报告！！！!%s: %s " %(time.ctime(),self.getName()))
                break

                
                
                    
                    
    

DF_wdpk = pd.DataFrame()

#-----WDPK处理线程---------------
class Consumer_wdpk(threading.Thread):
    def __init__(self,t_name,queue):
        threading.Thread.__init__(self,name=t_name)
        self.data=queue
    def run(self):
        while True:
            try:
                re_json = self.data.get(1,30) #get(self, block=True, timeout=None) ,1就是阻塞等待,30是超时30秒
                #print(type(re_json))
                conn = re_json[0]
                #cur = re_json[1]
                DJ_wdpk = re_json[1]
                StockCodePaper = re_json[2]
                
                if DJ_wdpk['QuoteType'] == 'WDPK':
                    recieve_time = time.strftime('%Y-%m-%d %X', time.localtime(time.time()))
                    
                    print('WDPK is Recieved about StockCodePaper ! %s ' %recieve_time)
                    #WDPK_DATA_LIST = []   
                    global DF_wdpk
                    
                    
                    for i in DJ_wdpk['Stocks']:
                        #print(DJ_wdpk['Stocks'])
                        if i['StockCode'] in StockCodePaper: 
                            #print(StockCodePaper)
                            #print(i['StockCode'])
                            
                            wdpk_data = []
                            #YMDHMC_data= time.strftime('%Y-%m-%d %X', time.localtime(i['Time']))#数据时间格式化
                            #YMDHMC_local = time.strftime('%Y-%m-%d %X', time.localtime(time.time()))#本地时间格式化
                            
                            dt = datetime.datetime.now()                           
                            
                            wdpk_data.extend([i['Time'],dt,i['StockCode'],i['StockName'],i['TotalAmount'],i['TotalVolume'],i['TotalDealtOrderNum']])
                            L1 = [i['High'],i['HighLimit'],i['Low'],i['LowLimit'],i['Open'],i['Price'],i['PrevClose']]
                            wdpk_data.extend([float('%.2f' %(x/100)) for x in L1])
                            wdpk_data.extend([i['SellVolume']])
                            wdpk_data.extend([float('%.2f' %(x/100)) for x in i['AskPrices']])
                            wdpk_data.extend([float(x) for x in i['AskVolumes']])
                            wdpk_data.extend([float('%.2f' %(x/100)) for x in i['BidPrices']])
                            wdpk_data.extend([float(x) for x in i['BidVolumes']])
                            
                            #print(type(wdpk_data))
                            #WDPK_DATA_LIST.append(wdpk_data)
                            #WDPK_DATA_LIST = list(set(tuple(WDPK_DATA_LIST))) #数据去重
                                                            
                            co0 = ['DateTime','LocalTime','Code','Name','TotalAmount','TotalVolume','TotalDealtOrderNum']
                            co1 = ['High','HighLimit','Low','LowLimit','Open','Close','Pre_close','SellVolume']
                            co2 = ['AskPrice1','AskPrice2','AskPrice3','AskPrice4','AskPrice5']
                            co3 = ['AskVolume1','AskVolume2','AskVolume3','AskVolume4','AskVolume5']
                            co4 = ['BidPrice5','BidPrice4','BidPrice3','BidPrice2','BidPrice1']
                            co5 = ['BidVolume5','BidVolume4','BidVolume3','BidVolume2','BidVolume1']
                            
                            columns = co0 + co1 + co2 + co3 + co4 + co5
                            #print(len(wdpk_data))
                            #print(len(columns))
                            Data = pd.DataFrame(wdpk_data)
                            Data_t = Data.T
                            Data_t.columns = columns
                            
                            Data_t.append(Data_t)
                            DF_wdpk = DF_wdpk.append(Data_t)
                            
                            #print(len(DF_wdpk))
                            #print('---------------------')
                    #print(DF_wdpk)
                    if len(DF_wdpk) >= 500:
                        pd.io.sql.to_sql(DF_wdpk,'dzh_wdpk', conn, schema='aliyun_stock', if_exists='append')
                        DF_wdpk.clear()

                    
                else:
                    self.data.put(re_json)
                    #time.sleep(0.2)
                    
        
            except:   #等待输入，超过30秒就报异常
                print( "%s: %s 普通盘口数据处理异常!" %(time.ctime(),self.getName()))
                #break

           
                
DF_sdpk = pd.DataFrame()
#-----SDPK处理线程---------------
class Consumer_sdpk(threading.Thread):
    def __init__(self,t_name,queue):
        threading.Thread.__init__(self,name=t_name)
        self.data=queue
    def run(self):
        while True:
            try:
                re_json = self.data.get(1,30) #get(self, block=True, timeout=None) ,1就是阻塞等待,30是超时30秒
                #print(type(re_json))
                conn = re_json[0]
                #cur = re_json[1]
                DJ_sdpk = re_json[1]
                StockCodePaper = re_json[2]
                global DF_sdpk
                #print( "%s: %s is consuming. %d in the queue is consumed!" % (time.ctime(),self.getName(),val_even))
                
                if (DJ_sdpk['QuoteType'] == 'SDPK') and (DJ_sdpk['StockCode'] in StockCodePaper):    #如果接收到SDPK的数据按以下方式处理
                
                    sdpk_data = []
                    #YMDHMC_local = time.strftime('%Y-%m-%d %X', time.localtime(time.time()))#本地时间格式化
                    dt = datetime.datetime.now()
                                                
                    sdpk_data.extend([dt,DJ_sdpk['StockCode'],DJ_sdpk['StockName'],int(DJ_sdpk['TotalAskVol']),int(DJ_sdpk['TotalBidVol']),float('%.2f' %DJ_sdpk['AvgAskPrice']),float('%.2f' %DJ_sdpk['AvgBidPrice'])])
                    sdpk_data.extend([float('%.2f' %x) for x in DJ_sdpk['AskPrices']])
                    sdpk_data.extend([int(x)for x in DJ_sdpk['AskVolumes']])
                    sdpk_data.extend([float('%.2f' %x) for x in DJ_sdpk['BidPrices']])
                    sdpk_data.extend([int(x) for x in DJ_sdpk['BidVolumes']])
                    #sdpk_data = tuple(sdpk_data)
                    #print(type(sdpk_data))
                    #SDPK_DATA_LIST.append(sdpk_data)
                    #SDPK_DATA_LIST = list(set(tuple(SDPK_DATA_LIST))) #数据去重
                    
                    co0 = ['DateTime','Code','Name','TotalAskVol','TotalBidVol','AvgAskPrice','AvgBidPrice']
                    #co1 = ['High','HighLimit','Low','LowLimit','Open','Close','Pre_close','SellVolume']
                    co2 = ['AskPrice6','AskPrice7','AskPrice8','AskPrice9','AskPrice10']
                    co3 = ['AskVolume6','AskVolume7','AskVolume8','AskVolume9','AskVolume10']
                    co4 = ['BidPrice6','BidPrice7','BidPrice8','BidPrice9','BidPrice10']
                    co5 = ['BidVolume6','BidVolume7','BidVolume8','BidVolume9','BidVolume10']
                    
                    columns = co0 + co2 + co3 + co4 + co5
                    #print(len(wdpk_data))
                    #print(len(columns))
                    Data = pd.DataFrame(sdpk_data)
                    Data_t = Data.T
                    Data_t.columns = columns
                    DF_sdpk = DF_sdpk.append(Data_t)
                    #print(Data_t)
                    #print('---------------------')
                
                if len(DF_sdpk) >= 500:
                    pd.io.sql.to_sql(DF_sdpk,'dzh_sdpk', conn, schema='aliyun_stock', if_exists='append')
                    Df_sdpk.clear()
                    
                        
      
                else:
                    self.data.put(re_json)
                    #time.sleep(0.2)
        
            except:   #等待输入，超过30秒就报异常
                print( "%s: %s 深度盘口数据处理异常!" %(time.ctime(),self.getName()))
                #break
       

'''
#-----ZBCJ处理线程(实时数据，数据量大下载较慢，安排盘后下载)---------------
class Consumer_zbcj(threading.Thread):
    def __init__(self,t_name,queue):
        threading.Thread.__init__(self,name=t_name)
        self.data=queue
    def run(self):
        while True:
            try:
                re_json = self.data.get(1,30) #get(self, block=True, timeout=None) ,1就是阻塞等待,5是超时5秒
                #print(type(re_json))
                conn = re_json[0]
                cur = re_json[1]
                DJ_zbcj = re_json[2]
                StockCodePaper = re_json[3]
                
                if  DJ_zbcj['QuoteType'] == 'ZBCJ':    #如果接收到ZBCJ的数据按以下方式处理
                    print('ZBCJ is Recieved about StockCodePaper !')
                    ZBCJ_DATA_LIST = []
                    for i in range(len(StockCodePaper)):
                        for i in DJ_zbcj['ZbData']:
                            if DJ_zbcj['StockCode'] in StockCodePaper:
                                
                                zbcj_data = []
                                YMDHMC = time.strftime('%Y-%m-%d %X', time.localtime(time.time()))#本地时间格式化
                                
                                zbcj_data = [YMDHMC,DJ_zbcj['StockCode'],DJ_zbcj['StockName'],i['Time'],i['ID'],float('%.2f' %i['Price']),i['Volume']]
                                #print(ZBCJ_DATA_LIST)
                                zbcj_data = tuple(zbcj_data)
                                ZBCJ_DATA_LIST.append(zbcj_data)
                                #print(ZBCJ_DATA_LIST)
                                ZBCJ_DATA_LIST = list(set(ZBCJ_DATA_LIST)) #数据去重
                             
                    sql = "insert into zbcj() values(%s,%s,%s,%s,%s,%s,%s)"
                    
                    cur.executemany(sql,ZBCJ_DATA_LIST)
                    conn.commit()
          
                else:
                    self.data.put(re_json)
                    #time.sleep(0.2)
        
            except:   #等待输入，超过30秒就报异常
                print( "%s: %s 逐笔成交数据处理异常!" %(time.ctime(),self.getName()))
                break

'''
#主线程
def main():
    
    #exit_flag = threading.Event()
    #exit_flag.clear()   

    queue = Queue()
    producer = Producer('Pro.', queue)
    http_get = Http_get('Http.',queue)
    consumer_wdpk = Consumer_wdpk('Con_wdpk.', queue)
    consumer_sdpk = Consumer_sdpk('Con_sdpk.', queue)
    #consumer_zbcj = Consumer_zbcj('Con_zbcj.', queue)
  
    producer.start()
    http_get.start()
    consumer_wdpk.start()
    consumer_sdpk.start()
    #consumer_zbcj.start()
    
    producer.join()
    http_get.join()
    consumer_wdpk.join()
    consumer_sdpk.join()
    #consumer_zbcj.join()
    
    '''
    #数据处理完毕，关闭数据库-------------------
    cur.close()  
    conn.close()
    '''

    print ('全部线程终止')

#执行程序
if __name__ == '__main__':
  main()

  
            








