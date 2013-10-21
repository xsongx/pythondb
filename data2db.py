# -*- coding: utf-8 -*-
"""
Created on Mon Oct 21 17:16:01 2013

@author: xsongx
"""

from weibo_tang import weibodb
import os
from pattern.db import date, time
def usermap(filename):
    f=open(filename,'r')
    usr_id=[]
    usr_id=[uid for uid in f]
    return usr_id
def user2dict(filename,mapid,db):
    f=open(filename,'r')
    filelines=f.readlines()
    targetlines=filelines[13:]
    print ''.join(targetlines[:100])
    mapdict={}
    for (i,uid) in enumerate(mapid):
        mapdict[uid]=i
    while(1):
        try:
            userdict={}
            sublines=targetlines[:14]
            if sublines[0] in mapdict.keys():
                userdict["mapid"]=mapdict[sublines[0]]
            else:
                userdict["mapid"]=0
            userdict["biFollowersCount"]=int(sublines[1])
            userdict["city"]=sublines[2]
            userdict["verified"]=sublines[3]
            userdict["followersCount"]=int(sublines[4])
            userdict["location"]=sublines[5]
            userdict["province"]=sublines[6]
            userdict["friendsCount"]=int(sublines[7])
            userdict["name"]=sublines[8]
            userdict["gender"]=sublines[9]
            userdict["createdAt"]=date(sublines[10])
            userdict["verifiedType"]=sublines[11]
            userdict["statusesCount"]=int(sublines[12])
            userdict["description"]=sublines[13]
            db.userInsert(userdict)
            targetlines=targetlines[14:]
        except:
            pass
                

mydb=weibodb("tangdb")
user_map=usermap("/media/work_/tangjie/user_list.txt")
user2dict("/media/work_/tangjie/user_profile1.txt",user_map,mydb)
user2dict("/media/work_/tangjie/user_profile2.txt",user_map,mydb)              
    