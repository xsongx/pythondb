# -*- coding: utf-8 -*-
"""
Created on Mon Oct 21 14:53:51 2013

@author: xsongx
"""

import os, sys; sys.path.insert(0, os.path.join("..", ".."))
from pattern.db import Database,MYSQL
from pattern.db import field, pk, STRING,UNIQUE,INTEGER,DATE,TEXT
from pattern.db import filter,all
from MySQLdb import IntegrityError
import MySQLdb
class weibodb(Database):
    def __init__(self, database):
        self.db=Database(database,password = '20090924',type=MYSQL)
        self.userTable()
        self.relationTable()
        self.retweetTable()
        self.originalTable()
        self.wordTable()
    def userTable(self):
        if not "usertable" in self.db:
            self.db.create("usertable",fields=(
             pk(), # Auto-incremental id.
             field("mapid",INTEGER),#用户映射id
             field("userid",STRING(20),index=UNIQUE),#用户UID
             field("name",STRING(50)),#友好显示名称
             field("province",STRING(20)),#用户所在省级ID
             field("city",STRING(20)),#	int	用户所在城市ID
             field("location",STRING(50)),#	string	用户所在地
             field("description",TEXT),#	string	用户个人描述
             field("gender",STRING(10)),#	string	性别，m：男、f：女、n：未知
             field("followersCount",INTEGER),#	int	粉丝数
             field("friendsCount",INTEGER),#	int	关注数
             field("statusesCount",INTEGER),#	int	微博数
             field("createdAt",DATE),#	string	用户创建（注册）时间
             field("verified",STRING(10)),#	STRING(10)	是否是微博认证用户，即加V用户，true：是，false：否
             field("verifiedType",STRING(20)),#	int	暂未支持
             field("biFollowersCount",INTEGER)#	int	用户的互粉数
            ))
            
    def wordTable(self):
        if not "wordtable" in self.db:
            self.db.create("wordtable",fields=(
             pk(), # Auto-incremental id.
             field("mapid",INTEGER,index=UNIQUE),
             field("freq",INTEGER),
             field("word",TEXT)
             ))
    def relationTable(self):
        if not "relationtable" in self.db:
            self.db.create("relationtable",fields=(
             pk(), # Auto-incremental id.
             field("user_v1",INTEGER),#用户1UID
             field("user_v2",INTEGER),#用户2UID
             field("re_0",INTEGER,default=-1),#初始关系
             field("re_1",INTEGER,default=-1),#关系变化
             field("re_2",INTEGER,default=-1),#关系变化
             field("re_3",INTEGER,default=-1),#关系变化
             field("re_4",INTEGER,default=-1),#关系变化
             field("re_5",INTEGER,default=-1),#关系变化
             field("re_6",INTEGER,default=-1),#关系变化
             field("re_7",INTEGER,default=-1),#关系变化
             field("re_8",INTEGER,default=-1),#关系变化
             field("re_9",INTEGER,default=-1),#关系变化
             field("re_10",INTEGER,default=-1),#关系变化
             field("re_11",INTEGER,default=-1),#关系变化
             field("re_12",INTEGER,default=-1),#关系变化
             field("re_13",INTEGER,default=-1),#关系变化
             field("re_14",INTEGER,default=-1),#关系变化
             field("re_15",INTEGER,default=-1),#关系变化
             field("re_16",INTEGER,default=-1),#关系变化
             field("re_17",INTEGER,default=-1),#关系变化
             field("re_18",INTEGER,default=-1),#关系变化
             field("re_19",INTEGER,default=-1),#关系变化
             field("re_20",INTEGER,default=-1),#关系变化
             field("re_21",INTEGER,default=-1),#关系变化
             field("re_22",INTEGER,default=-1),#关系变化
             field("re_23",INTEGER,default=-1),#关系变化
             field("re_24",INTEGER,default=-1),#关系变化
             field("re_25",INTEGER,default=-1),#关系变化
             field("re_26",INTEGER,default=-1),#关系变化
             field("re_27",INTEGER,default=-1),#关系变化
             field("re_28",INTEGER,default=-1),#关系变化
             field("re_29",INTEGER,default=-1),#关系变化
             field("re_30",INTEGER,default=-1),#关系变化
             field("re_31",INTEGER,default=-1),#关系变化
             field("re_32",INTEGER,default=-1)#关系变化
            ))
    def retweetTable(self):
        if not "retwtable" in self.db:
            self.db.create("retwtable",fields=(
                pk(), # Auto-incremental id.
                field("sid",STRING(20),index=UNIQUE),
                field("origin_sid",STRING(20)),                
                field("uid",STRING(20)),
                field("createdAt",DATE),
                field("status",TEXT),
                field("mention",STRING(50)),
                field("rwfrom",STRING(50)),
                field("link",STRING(50))        
                ))
    def originalTable(self):
        if not "originaltable" in self.db:
            self.db.create("originaltable",fields=(
                pk(), # Auto-incremental id.
                field("sid",STRING(20),index=UNIQUE),
                field("uid",STRING(20)),
                field("createdAt",DATE),
                field("status",TEXT),
                field("mention",STRING(50)),
                field("link",STRING(50)),
                field("totalrw",INTEGER),
                field("rwnum",INTEGER)
                ))        
            
    def userInsert(self,userdict):
        try:
            self.db.usertable.append(
             mapid=userdict["mapid"],#用户映射id
             userid=userdict["userid"]#用户UID
             )
        except IntegrityError:
            print "User"+str(userdict['userid'])+" has existed!"
        except:
            print "error when inserting user recorder:"+str(userdict['userid'])+"!!!!"
            for kk,vv in userdict.items():
                print str(kk)+'='+str(vv)
            pass   
    def userUpdate(self,userdict):
        try:
            q=self.db.usertable.search(fields=['id','mapid','userid'],
                                       filters=all(filter("userid",userdict[0])))#
            for row in q.rows():
                a=row
            if (a):
                userdict.insert(0,a[1])
                userup=tuple(userdict)
#                self.db.usertable.update(a[0],userup)
                self.db.usertable.update(a[0],name=userdict[2],province=userdict[3],city=userdict[4],
                                         location=userdict[5],description=userdict[6],gender=userdict[7],
                                            followersCount=userdict[8],friendsCount=userdict[9],
                                            statusesCount=userdict[10],createdAt=userdict[11],
                                            verified=userdict[12],verifiedType=userdict[13],biFollowersCount=userdict[14])
                print "User "+str(userdict[1])+" has updated!"            
            else:
                userdict.insert(0,-1)
                userup=tuple(userdict)
                self.db.usertable.append(userup)
#                self.db.usertable.append(
#    #             mapid=userdict["mapid"],#用户映射id
#                 userid=userdict["userid"],#用户UID
#                 name=userdict["name"],#友好显示名称
#                 province=userdict["province"],#用户所在省级ID
#                 city=userdict["city"],#	int	用户所在城市ID
#                 location=userdict["location"],#	string	用户所在地
#                 description=userdict["description"],#	string	用户个人描述
#                 gender=userdict["gender"],#	string	性别，m：男、f：女、n：未知
#                 followersCount=userdict["followersCount"],#	int	粉丝数
#                 friendsCount=userdict["friendsCount"],#	int	关注数
#                 statusesCount=userdict["statusesCount"],#	int	微博数
#                 createdAt=userdict["createdAt"],#	string	用户创建（注册）时间
#                 verified=userdict["verified"],#	STRING(10)	是否是微博认证用户，即加V用户，true：是，false：否
#                 verifiedType=userdict["verifiedType"],#	int	暂未支持
#                 biFollowersCount=userdict["biFollowersCount"]#	int	用户的互粉数
#                 )
                print "user "+"User "+str(userdict[1])+" has appended!"
        except IntegrityError:
            print "User"+str(userdict['userid'])+" has existed!"         
            pass
        except:
            print "error when updating user recorder:"+str(userdict['userid'])+"!!!!"
            for kk,vv in userdict.items():
                print str(kk)+'='+str(vv)
            exit(0)
    def relBatchInsert(self,rellist):
        conn = MySQLdb.connect(host='localhost',user='root',passwd='20090924',charset='utf8')
        cursor = conn.cursor()
        try:
            DB_NAME = 'tangdb'
            conn.select_db(DB_NAME)
            cursor.executemany('INSERT INTO relationtable values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',rellist)
#            for rel in rellist:           
#                self.db.relationtable.append((rel))
            conn.commit()
        except:
            print "batch insert error!!"
            exit(0)
        cursor.close()
        conn.close()
        
    def wordInsert(self,wlist):
        conn = MySQLdb.connect(host='localhost',user='root',passwd='20090924',charset='utf8')
        cursor = conn.cursor()
        try:
            DB_NAME = 'tangdb'
            conn.select_db(DB_NAME)
            cursor.executemany('INSERT INTO wordtable values(%s,%s,%s,%s)',wlist)
            conn.commit()
        except:
            print "batch insert error!!"
            exit(0)
        cursor.close()
        conn.close()
#    def retweetInsert(self,rtlist):
#        conn = MySQLdb.connect(host='localhost',user='root',passwd='20090924',charset='utf8')
#        cursor = conn.cursor()        
        
#        except:
#            print "batch insert error!!"
#            pass
#==============================================================================
#         DB_NAME = 'tangdb'
#         conn.select_db(DB_NAME)
#         cursor.executemany('INSERT INTO retwtable values(%s,%s,%s,%s,%s,%s,%s,%s,%s)',rtlist)
#         conn.commit()        
#==============================================================================
#
#    def originalInsert(self,orlist):
#        conn = MySQLdb.connect(host='localhost',user='root',passwd='20090924',charset='utf8')
#        cursor = conn.cursor()
#        
       
    def relationInsert(self,reldict):
        try:
            q=self.db.relationtable.search(fields=['id'],filters=all(filter("user_v1",reldict[0]),filter("user_v2",reldict[1])))
            if (q.rows()):
                for row in q.rows():
                    a=row
                if (a):
                    self.db.relationtable.update(a[0],{reldict[-1]:0})
                    print "Relation between User "+str(reldict[0])+" and User "+str(reldict[1])+" has updated!"
                else:
                    self.db.relationtable.append({'user_v1':reldict[0],'user_v2':reldict[1],reldict[-1]:0})
                    print "Relation between User "+str(reldict[0])+" and User "+str(reldict[1])+" has inserted!"
            else:
                self.db.relationtable.append({'user_v1':reldict[0],'user_v2':reldict[1],reldict[-1]:0})
                print "Relation between User "+str(reldict[0])+" and User "+str(reldict[1])+" has inserted!"
        except:
            print "error when inserting relation recorder:"+str(reldict[0])+' and '+str(reldict[1])+"!!!!"
            pass
            exit(0)