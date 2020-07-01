# -*- coding: utf-8 -*-
import urllib
import requests
import numpy
from fake_useragent import UserAgent
import json
import pandas as pd
import time
import datetime
import os

# 发送get请求
"""
genres : 游戏类别
n_ratings: 评分人数
platforms: 平台
rating ： 评分
content : 最热评论
star : 星数
title : 游戏名称
"""

def getDoubanGame(genres):
    id_all1 = {1:"动作",5:"角色扮演",41:"横版过关",4:"冒险",48:"射击",32:"第一人称射击",
    2:"策略",18:"益智",7:"模拟",3:"体育",6:"竞速",9:"格斗",37:"乱斗/清版",12:"即时战略",
    19:"音乐/旋律"}

    comment_api = 'https://www.douban.com/j/ilmen/game/search?genres={}&platforms=&q=&sort=rating&more={}'

    headers = {"User-Agent": UserAgent(verify_ssl=False).random}

    response_comment = requests.get(comment_api.format(genres,1),headers = headers)
    json_comment = response_comment.text
    json_comment = json.loads(json_comment)
    col = ['name','star','rating','platforms','n_ratings','genres','content']

    dataall = pd.DataFrame()


    num = json_comment['total']
    print('{}类别共{}个游戏,开始爬取!'.format(id_all1[genres],num))

    i = 0
    while i < num:

        if i == 0:
            s = 1
        else:
            s = json_comment['more']

        response_comment = requests.get(comment_api.format(genres,s),headers = headers)
        json_comment = response_comment.text
        json_comment = json.loads(json_comment)

        n = len(json_comment['games'])
        datas = pd.DataFrame(index = range(n),columns = col)
        for j in range(n):
            datas.loc[j,'name'] = json_comment['games'][j]['title']
            datas.loc[j,'star'] = json_comment['games'][j]['star']
            datas.loc[j,'rating'] = json_comment['games'][j]['rating']
            datas.loc[j,'platforms'] = json_comment['games'][j]['platforms']
            datas.loc[j,'n_ratings'] = json_comment['games'][j]['n_ratings']
            datas.loc[j,'genres'] = json_comment['games'][j]['genres']
            datas.loc[j,'content'] = json_comment['games'][j]['review']['content']

            i += 1
        dataall = pd.concat([dataall,datas],axis = 0)
        print('已完成 {}% !'.format(round(i/num*100,2)))
        time.sleep(0.5)
    dataall = dataall.reset_index(drop = True)
    dataall['type'] = id_all1[genres]
    return dataall

id_all = {"动作":1,"角色扮演" :5,"横版过关" :41,"冒险" :4,"射击": 48,"第一人称射击":32,
"策略":2,"益智":18,"模拟":7,"体育":3,"竞速":6,"格斗":9,"乱斗/清版":37,"即时战略":12,"音乐/旋律":19}


id_all1 = {1:"动作",5:"角色扮演",41:"横版过关",4:"冒险",48:"射击",32:"第一人称射击",
2:"策略",18:"益智",7:"模拟",3:"体育",6:"竞速",9:"格斗",37:"乱斗/清版",12:"即时战略",
19:"音乐/旋律"}

for i in list(id_all.values()):
    dataall = getDoubanGame(i)
    filename = '游戏类别_' + id_all1[i] +'.csv'
    filename = filename.replace('/','')
    dataall.to_csv(filename)