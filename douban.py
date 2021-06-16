import requests
from bs4 import BeautifulSoup
import numpy as np
import os
import traceback
import time
import pymysql

def get_urls():
    pages = np.arange(0, 250, 25)
    urls = []
    for i in pages:
        url = 'https://movie.douban.com/top250?start={}'.format(i)
        urls.append(url)
    return urls


def get_contents(url, headers):
    try:
        r = requests.get(url, headers=headers)
        html = r.content.decode()
    except:
        print("Error!")
    return html


def get_movies(urls,headers):
    name_list=[]
    details_list=[]
    comment_list=[]
    describe_list=[]
    rate_list=[]
    for i in urls:
        r = get_contents(i,headers)
        soup = BeautifulSoup(r, 'html.parser')
        for i in soup.select('#content > div > div.article > ol div > div.info > div.hd > a > span:nth-child(1)'):
            # print(i.get_text())
            name_list.append(i.get_text())
        for i in soup.select('#content > div > div.article > ol div > div.info > div.bd > p:nth-child(1)'):
            # print(i.get_text())
            text = "".join(i.get_text().split())
            details_list.append(text)
        for i in soup.select('#content > div > div.article > ol  div > div.info > div.bd > div > span:nth-child(4)'):
           # print(i.get_text())
           comment_list.append(i.get_text())
        for i in soup.select('#content > div > div.article > ol  div > div.info > div.bd > div > span.rating_num'):
           # print(i.get_text())
           rate_list.append(float(i.get_text()))
        # for i in soup.select('#content > div > div.article > ol  div > div.info > div.bd > p.quote > span'):
        #     # print(i.get_text())
        #     describe_list.append(i.get_text())
        # name_selector = '#content > div > div.article > ol div > div.info > div.hd > a > span:nth-child(1)'
        # name_list = [x.get_text() for x in soup.select(name_selector)]
        # details_selector = '#content > div > div.article > ol div > div.info > div.bd > p:nth-child(1)'
        # details_list = ["".join(x.get_text().split()) for x in soup.select(details_selector)]
        # comment_selector = '#content > div > div.article > ol  div > div.info > div.bd > div > span:nth-child(4)'
        # comment_list = [x.get_text() for x in soup.select(comment_selector)]
        # rate_selector = '#content > div > div.article > ol  div > div.info > div.bd > div > span.rating_num'
        # rate_list = [float(x.get_text()) for x in soup.select(rate_selector)]
        # describe_selector ='#content > div > div.article > ol  div > div.info > div.bd > p.quote > span'
        # describe_list = [x.get_text() for x in soup.select(describe_selector)]
        # img_selector = '#content > div > div.article > ol  div > div.pic > a > img'
        # img_list = [x.get('src') for x in soup.select(img_selector)]
        # print(len(content_list))
        time.sleep(0.2)
    contents = list(zip(name_list,details_list,comment_list,rate_list))
    if check_sql() == 0:
        print("following insert data")
        insert_data(contents)
def make_dir(path):
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)
        print('---  new folder ---')
        print('---  ok  ---')
    else:
        print('--- folder exists ---')


def download_img(url, headers, name, path):
    os.chdir(path)
    r = requests.get(url, headers=headers)
    try:
        with open(name, "wb")as f:
            f.write(r.content)
    except:
        print("Error!")

def close_conn(conn, cursor):
    if cursor:
        cursor.close()
    if conn:
        conn.close()

def get_conn():
    """
    :return: 连接，游标
    """
    # 创建连接
    conn = pymysql.connect(host="127.0.0.1",
                           user="root",
                           password="zh332233",
                           db="bilibili",
                           charset="utf8")
    # 创建游标
    cursor = conn.cursor()  # 执行完毕返回的结果集默认以元组显示
    return conn, cursor

def insert_data(contents):
    cursor = None
    conn = None
    try:
        print(f"{time.asctime()}开始插入数据")
        conn, cursor = get_conn()
        sql = "insert into douban (name,details,comment,rate) values (%s,%s,%s,%s)"
        cursor.executemany(sql,contents)
        conn.commit()
        print(f"{time.asctime()}插入完毕")
    except:
        traceback.print_exc()
    finally:
        close_conn(conn, cursor)

def check_sql():
    cursor = None
    conn = None
    try:
        conn,cursor = get_conn()
        sql = "select count(*) from douban"
        cursor.execute(sql)
        data = cursor.fetchall()
        if data[0][0] > 0:
            print('--- data already exists ---')
            data = 1
        else:
            data = 0
    except :
        print('Error!')
    finally:
        close_conn(conn, cursor)
    return data

headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36'}
path = 'C:\\Users\\12370\\Desktop\\douban\\img'
if __name__ == '__main__':
    urls = get_urls()
    make_dir(path)
    get_movies(urls,headers)
