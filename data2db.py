# -*- coding: utf-8 -*-
"""
Created on Mon Oct 21 17:16:01 2013

@author: xsongx
"""


from weibo_tang import weibodb
import codecs
from pattern.db import date,DateError
from BeautifulSoup import *
import pickle
import chardet

def zh2unicode(stri): 
    fwr=open('/media/M_fM__VM_0M_eM__JM__M_eM__MM_7/tangjie/weibocontents/wordtable_error.txt','a')
    c=chardet.detect(stri)
    """Auto converter encodings to unicode It will test utf8,gbk,big5,jp,kr to converter""" 
    if c in ('utf-8', 'gbk', 'big5', 'euc_kr','utf16','utf32','GB2312','EUC-JP','EUC-TW'):
#    '''windows-1251','IBM855','windows-1252','TIS-620','IBM866''KOI8-R','SHIFT_JIS','windows-1253','ISO-8859-8','ISO-8859-5',
#    'ISO-8859-7','windows-1255','MacCyrillic','ISO-8859-2'): '''
#        encc = c
        try: 
            return stri.decode(c).encode('utf-8') 
        except:
            fwr.write(stri+'\n')
            pass
    else:
        try: 
            return stri.decode('gbk').encode('utf-8') 
        except:
            fwr.write(stri+'\n')
            pass
    fwr.close()
    return None
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
def wordTable(filename):
    worddict={}
    wtfile=open(filename,'r')
    wordtable=pickle.load(wtfile)
    total=len(wordtable)
    print total
    for wordrec in wordtable:
        worddict[wordrec[0]]=(wordrec[1],wordrec[2])
    wdfile=open('/media/M_fM__VM_0M_eM__JM__M_eM__MM_7/tangjie/weibocontents/worddict.pickle','w')
    pickle.dump(worddict,wdfile)
def wordTableInsert(filename,db):
    wtfile=open(filename,'r')    
    wordtable=pickle.load(wtfile)
    wordlist=[]
    print len(wordtable)
    wordlist=[(i+1,int(w[0]),int(w[1]),w[2]) for (i,w) in enumerate(wordtable)]
#    for w in wordtable:
#        wordlist.append(list([int(w[0]),int(w[1]),w[2]]))
    it=0
    while (it<len(wordlist)):
        try:
            insertlist=wordlist[it:it+10000]
            it=it+10000
            db.wordInsert(insertlist)
            print it
        except:
            insertlist=wordlist[it:]
            db.wordInsert(insertlist)
    wtfile.close()
def retweetInsert(filename,db):  
    f=open(filename,'rb')
    forigin=open('/media/M_fM__VM_0M_eM__JM__M_eM__MM_7/tangjie/weibocontents/orgin.pickle','w')
    t=True
    c=list(['@','retweet','link'])
    originaldict={}
    i=1
    m=0
    rtstr=f.readline()
    rtAll=[]
    dupRt=[]
    while t:
        try:            
            instr=zh2unicode(rtstr.strip())
            if instr!=None:
#                print instr
                ortw=instr.split()                
                ortwid=int(ortw[0])
                ortwuid=int(ortw[1])
                timelist=ortw[2].strip().split('-')
                timestr='-'.join(timelist[:-1])+' '+timelist[-1]
                orcreAt=date(timestr)
                orRt=int(ortw[-1])
                rt=int(f.readline().strip())
                originaldict[ortwid]=list([ortwuid,orcreAt,orRt,rt])                
                rwlist=[]
                rtstr=zh2unicode(f.readline().strip())
                for j in range(rt):                    
                    rtlist=rtstr.split()
                    rtuid=int(rtlist[0])
                    timelist=rtlist[1].strip().split('-')
                    timestr='-'.join(timelist[:-1])+' '+timelist[-1]
                    rtcreAt=date(timestr)
                    rtid=int(rtlist[-1])
                    rtTw=zh2unicode(f.readline().strip())
                    rtstr=zh2unicode(f.readline().strip())
                    mention=''
                    rtfrom=''
                    link=''
                    while rtstr.split()[0] in c:
                        if rtstr.split()[0]=='@':
                            mention=' '.join(rtstr.split()[1:])
                        if rtstr.split()[0]=='retweet':
                            rtfrom=' '.join(rtstr.split()[1:])
                        if rtstr.split()[0]=='link':
                            link=' '.join(rtstr.split()[1:])
                        rtstr=zh2unicode(f.readline().strip())
                    rtrecord=(i+j,rtid,ortwid,rtuid,rtcreAt,rtTw,mention,rtfrom,link)
                    if rtid in rtAll:
                        dupRt.append(rtrecord)
                    else:
                        rwlist.append(rtrecord) 
                        rtAll.append(rtid)
                db.retweetInsert(rwlist)
                i=i+rt
                m=m+1
                print str(m)+':retweet of '+str(ortwid)+' has finished: '+str(rt)
        except:
            t=False            
            pass
    pickle.dump(originaldict,forigin)
    fdup=open('/media/M_fM__VM_0M_eM__JM__M_eM__MM_7/tangjie/weibocontents/dupretweet.txt','w')
    duplist=['\t'.join(r) for r in dupRt]
    dup='\n'.join(duplist)
    fdup.write(dup)
    fdup.close()
    print len(originaldict.keys())
    forigin.close()
    print 'retweet over!'
def oringinalTwInsert(filename,db):
    f=open(filename,'rb')
    forigin=open('/media/M_fM__VM_0M_eM__JM__M_eM__MM_7/tangjie/weibocontents/orgin.pickle','r')
    originaldict=pickle.load(forigin)
    t=True
    c=list(['@','link'])
    i=1
    orstr=f.readline()
    orAll=[]
    dupOr=[]
    orlist=[]
    while t:
        try:            
            instr=zh2unicode(orstr.strip())
            if instr!=None:
#                print instr
                ortw=instr.split()                
                ortwid=int(ortw[0])
                try:
                    origlist=originaldict[ortwid]
                except:
                    print 'key error: '+str(ortwid)
                    exit(0)
                ortwuid=origlist[0]
                orcreAt=origlist[1]
                orRt=origlist[2]
                rt=origlist[3]
                ortweet=zh2unicode(f.readline().strip())                
                orstr=zh2unicode(f.readline().strip())
                mention=''
                link=''
                while orstr.split()[0] in c:
                    if orstr.split()[0]=='@':
                        mention=' '.join(rtstr.split()[1:])
                    if orstr.split()[0]=='link':
                        link=' '.join(rtstr.split()[1:])
                    orstr=zh2unicode(f.readline().strip())
                orRecord=(i,ortwid,ortwuid,orcreAt,ortweet,mention,link,orRt,rt)
                if rtid in orAll:
                    dupOr.append(orRecord)
                else:
                    orlist.append(orRecord) 
                    orAll.append(rtid)              
                i=i+1
                print 'tweet: '+str(ortwid)+' has finished!'
        except:
            t=False            
            pass
    while (it<len(orlist)):
        try:
            insertlist=orlist[it:it+10000]
            it=it+10000
            db.originalInsert(insertlist)
            print it
        except:
            insertlist=orlist[it:]
            db.originalInsert(insertlist)
    dup=open('/media/M_fM__VM_0M_eM__JM__M_eM__MM_7/tangjie/weibocontents/duporigin.txt','w')
    duplist=['\t'.join(r) for r in dupOr]
    dup='\n'.join(duplist)
    fdup.write(dup)
    fdup.close()
mydb=weibodb("tangdb")
#user_map=usermap("/media/M_fM__VM_0M_eM__JM__M_eM__MM_7/tangjie/uidlist.txt",mydb)
#user2db("/media/M_fM__VM_0M_eM__JM__M_eM__MM_7/tangjie/user_profile1-1.txt",mydb)
#user2db("/media/M_fM__VM_0M_eM__JM__M_eM__MM_7/tangjie/user_profile2-1.txt",mydb)
#staticRel("/media/M_fM__VM_0M_eM__JM__M_eM__MM_7/tangjie/weibo_network.txt",mydb)
#dynamicRel("/media/M_fM__VM_0M_eM__JM__M_eM__MM_7/tangjie/graph_170w_1month.txt",mydb)
#wordTableInsert('/media/M_fM__VM_0M_eM__JM__M_eM__MM_7/tangjie/weibocontents/WordTable.txt',mydb)    
retweetInsert('/media/M_fM__VM_0M_eM__JM__M_eM__MM_7/tangjie/weibocontents/Retweet_Content.txt',mydb)         
oringinalTwInsert('/media/M_fM__VM_0M_eM__JM__M_eM__MM_7/tangjie/weibocontents/Root_Content.txt',mydb)    