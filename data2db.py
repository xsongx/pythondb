# -*- coding: utf-8 -*-
"""
Created on Mon Oct 21 17:16:01 2013

@author: xsongx
"""

from weibo_tang import weibodb
import codecs
from pattern.db import date,DateError
def usermap(filename,dbname):
    f=codecs.open(filename,'rb',encoding='UTF-8')
#    f = codecs.EncodedFile(f,file_encoding = "utf8",data_encoding ="gb2312")
    usr_id=[]
    usr_id=[uid.strip() for uid in f]
    mapdict={}
    for (i,uid) in enumerate(usr_id):
        mapdict["userid"]=uid
        mapdict["mapid"]=i
        dbname.userInsert(mapdict)
        print uid+':'+str(i)
#    return mapdict
def user2dict(filename):
    f=codecs.open(filename,'rb',encoding='UTF-8')
#    f = codecs.EncodedFile(f,file_encoding = "utf8",data_encoding ="gb2312")
    filelines=f.readlines()
    print ''.join(filelines[:14])
    i=15
    while(i<len(filelines)):
#        print "line"+str(i)
        try:
            
            try:
                sublines=filelines[i:i+15]
            except:
                sublines=filelines[i:]
#            print ''.join(sublines)
#            if sublines[0].strip() in mapid.keys():
#                userdict["mapid"]=mapid[sublines[0].strip()]
#            else:
#                userdict["mapid"]=-1
#            print sublines[0].strip()+' mapto: '+str(userdict["mapid"])
            userid=sublines[0].strip()
            try:
                biFollowersCount=int(sublines[1].strip())
            except:
                biFollowersCount=0
            city=sublines[2].strip()
            verified=sublines[3].strip()
            try:
                followersCount=int(sublines[4].strip())
            except:
                followersCount=0
            location=sublines[5].strip()
            province=sublines[6].strip()
            friendsCount=int(sublines[7].strip())
            name=sublines[8].strip().replace("\\","\\\\")
            gender=sublines[9].strip()
            timelist=sublines[10].strip().split('-')
            timestr='-'.join(timelist[:-1])+' '+timelist[-1]
            createdAt=date(timestr)
            verifiedType=sublines[11].strip()
            try:
                statusesCount=int(sublines[12].strip())
            except:
                statusesCount=0
            description=sublines[13].strip().replace("\\","\\\\")
            i=i+15
            userrec=[]
            userrec.append(userid)
            userrec.append(name)
            userrec.append(province)
            userrec.append(city)
            userrec.append(location)
            userrec.append(description)
            userrec.append(gender)
            userrec.append(followersCount)
            userrec.append(friendsCount)
            userrec.append(statusesCount)
            userrec.append(createdAt)
            userrec.append(verified)
            userrec.append(verifiedType)
            userrec.append(biFollowersCount)
            yield userrec
        except:
            print filename +" error!"
            exit(0)
#        except:
#            pass

def user2db(userprofile,dbname):
    a=True
    user_dict=user2dict(userprofile)
    while a:
        try:
            user=user_dict.next()
            dbname.userUpdate(user)
        except StopIteration:
            a=False
            print userprofile+" Has been processed!"
#                fileprocessed.write(f+'\n')
            pass
#        except :
#            print "error!!!"
#            pass
            
            
def staticRel(filename,db):
    f=open(filename,'r')
#    flines=f.readlines(30)
#    print ''.join(flines)
    line1=f.readline().strip()
    num=line1.split()
    i=0
    j=0
    while(i<int(num[0])):
        rel_line=f.readline().split()
        reldict={}
        userv1=int(rel_line[0])
        reldict['user_v1']=userv1
        followees=int(rel_line[1])
        rel_line=rel_line[2:]
        userrel=[]
#        for rel in range(followees):
#            reldict['user_v2']=int(rel_line[2*rel])
#            reldict['re_0']=int(rel_line[2*rel+1])
#            db.relationInsert(reldict)
        for rel in range(followees): 
            j=j+1
            user_v2=int(rel_line[2*rel])
            re_0=int(rel_line[2*rel+1])
            userrel.append((j,userv1,user_v2,re_0,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1))
        db.relBatchInsert(userrel)
        i=i+1
        print str(j)+":"+str(userv1)+" static!"

def dynamicRel(filename,db):
    f=open(filename,'r')
    t=True
    while t:
        try:
            fline=f.readline().strip()
            rel_line=fline.split()
            user_v1=int(rel_line[0])
            user_v2=int(rel_line[1])
            rel="re_"+rel_line[-1]
            reldict=(user_v1,user_v2,rel)
            db.relationInsert(reldict)
        except:
            t=False
            print "over!"
            exit(0)

mydb=weibodb("tangdb")
#user_map=usermap("/media/M_fM__VM_0M_eM__JM__M_eM__MM_7/tangjie/uidlist.txt",mydb)
#user2db("/media/M_fM__VM_0M_eM__JM__M_eM__MM_7/tangjie/user_profile1-1.txt",mydb)
#user2db("/media/M_fM__VM_0M_eM__JM__M_eM__MM_7/tangjie/user_profile2-1.txt",mydb)
#staticRel("/media/M_fM__VM_0M_eM__JM__M_eM__MM_7/tangjie/weibo_network.txt",mydb)
dynamicRel("/media/M_fM__VM_0M_eM__JM__M_eM__MM_7/tangjie/graph_170w_1month.txt",mydb)
             
    