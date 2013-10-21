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
class weibodb(Database):
    def __init__(self, database):
        self.db=Database(database,password = '20090924',type=MYSQL)
        self.userTable()
#        self.statusTable()
#        self.networkTable()
    def userTable(self):
        if not "usertable" in self.db:
            self.db.create("usertable",fields=(
             pk(), # Auto-incremental id.
             field("mapid",INTEGER,index=UNIQUE),#用户映射id
             field("userid",STRING(20)),#用户UID
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
    def userInsert(self,userdict):
        for f in self.db.usertable.fields:
            if f not in userdict.keys():
                userdict[f]=u''
        try:
            self.db.usertable.append(
             mapid=userdict["mapid"],#用户映射id
             userid=userdict["id"],#用户UID
             name=userdict["name"],#友好显示名称
             province=userdict["province"],#用户所在省级ID
             city=userdict["city"],#	int	用户所在城市ID
             location=userdict["location"],#	string	用户所在地
             description=userdict["description"],#	string	用户个人描述
             gender=userdict["gender"],#	string	性别，m：男、f：女、n：未知
             followersCount=userdict["followersCount"],#	int	粉丝数
             friendsCount=userdict["friendsCount"],#	int	关注数
             statusesCount=userdict["statusesCount"],#	int	微博数
             createdAt=userdict["createdAt"],#	string	用户创建（注册）时间
             verified=userdict["verified"],#	STRING(10)	是否是微博认证用户，即加V用户，true：是，false：否
             verifiedType=userdict["verifiedType"],#	int	暂未支持
             biFollowersCount=userdict["biFollowersCount"]#	int	用户的互粉数
             )
        except IntegrityError:
            print "User"+str(userdict['id'])+" has existed!"
            q=self.db.usertable.search(fields=['mapid','province','city','location','description','gender','verified','verifiedType'],
                                       filters=all(filter("userid",userdict['id'])))
            for row in q.rows():
                a=row
            for (i,fieldid) in enumerate(q.fields):
                if (a[i]==u'null' or a[i]==u''):
                    if (userdict[fieldid]!=u'null' and userdict[fieldid]!=u''):
                        self.db.usertable.update(a[0],{fieldid:userdict[fieldid]})
                        print "User"+str(userdict['id'])+fieldid+" has updated!"
            pass
        except TypeError:
            print "error when inserting user recorder:"+str(userdict['id'])+"!!!!"
            exit(0)