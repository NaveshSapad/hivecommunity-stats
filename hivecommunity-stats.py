#!/usr/bin/env python
# coding: utf-8

# 

# In[83]:


from beem import Hive
from beem import Steem
from beem.transactionbuilder import TransactionBuilder
from beembase import operations
from beem.instance import set_shared_steem_instance
from hiveengine.wallet import Wallet
import requests
import pandas as pd
import pyodbc
import json
from datetime import datetime as dt
from datetime import timedelta
import time
import os
import streamlit as st


def establish_connection(uid,pwd):

    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=vip.hivesql.io;'
                      'Database=DBHive;'
                      'uid='+str(uid)+';'
                      'pwd='+str(pwd)+';'
                      'Trusted_Connection=no;' )
    
    #conn = pymssql.connect(server='vip.hivesql.io', user=uid, password=pwd, database='DBHive')

    return(conn)

def get_frontend(sym):
    if(sym=='LEO'):
        frontend='leofinance'
    elif(sym=='SPORTS'):
        frontend='sportstalksocial'
    elif(sym=='CTP'):
        frontend='ctptalk'
        
    return frontend

def retrieve_query(user,conn):
    comment_query = pd.read_sql_query(''' select * from Comments  where author='{}' and parent_author <> '' and created > GETDATE()-1 ORDER BY created DESC '''.format(user),conn)

    post_query = pd.read_sql_query(''' select * from Comments  where author='{}' and parent_author = '' and created > GETDATE()-1 ORDER BY created DESC '''.format(user),conn)
    
    comment_query=comment_query[comment_query['created']>str(dt.utcnow().date())]
    post_query=post_query[post_query['created']>str(dt.utcnow().date())]
    
    return comment_query,post_query

def retrieve_data(comment_query,post_query,frontend):
    
    
    #for i in range(0,len(post_query)):
        
    
    list_body=[]
    parent_author_name=[]
    sum_len=0
    start_time=''
    end_time=''
    number_of_comments=0
    flag=0
    authors_talked_to=0
    hours=0
    
    number_p=len(post_query)
    number_c=len(comment_query)
    
    left_box.write("Total Posts today from all front-ends:"+str(number_p))
    left_box.write("Total Comments today from all front-ends:"+str(number_c))
    
    print("\n Below data is only with respect to those which you have posted from {} frontend and only this will be considered for rewards.".format(frontend))
    for i in range(0,len(comment_query)):
        if(json.loads(comment_query['json_metadata'][i])['app'].startswith('leofinance')): # frontend
            flag=1
            s=(comment_query['body'][i])
            list_body.append(s)


            parent_author_name.append(comment_query['parent_author'][i])

            if not end_time:
                end_time=comment_query['created'][i]

            number_of_comments += 1 


    if(flag==1):
        start_time=comment_query['created'][i]


        for body in list_body:
            body=body.split("Posted Using")[0]
            sum_len += len(body)

        authors_talked_to=len(set(parent_author_name))

        total_time=end_time-start_time
        hours= total_time.seconds / 3600
    
    return number_of_comments,sum_len,hours,authors_talked_to

def calculation(number_of_comments,sum_len,hours,authors_talked_to,sym):
    right_box.write("\nAuthors talked to:"+str(authors_talked_to)+"Total length:"+str(sum_len)+"Number of comments:"+str(number_of_comments)+"Total time:"+str(hours))

    quantity_p = (authors_talked_to * 0.005) + (sum_len * 0.0001) + (number_of_comments * 0.0075)


    if(quantity_p>1):
        quantity_p=1

    #right_box.write("\n"+str(quantity_p),(authors_talked_to * 0.005),sum_len * 0.0001,(number_of_comments * 0.0075))

    funds=get_balance('amr008.rewards',sym)
    quantity=float(funds)*(quantity_p/100)

    quantity=round(quantity,3)
    print(quantity)
    
    return quantity

def get_balance(hive_user,token):
    wallet=Wallet(hive_user)

    list_balances=wallet.get_balances()
    for i in range(0,len(list_balances)):
        if(list_balances[i]['symbol']==token):
            return(list_balances[i]['balance'])
    return(0)

def transaction_check(user,number_of_comments,sum_len,authors_talked_to,hours,quantity,sym):
    df=pd.read_csv('user_claim.csv')
    df=df[df['date']==str(dt.utcnow().date())]
    df=df[df['sym']==sym]

    if user not in df['user'].tolist():
        if(number_of_comments>=10):
            right_box.write("Number of comments satisfied")
            if(sum_len>1000):
                right_box.write("Quality of comments satisfied")
                if(authors_talked_to>4):
                    right_box.write("You have talked to atleast 5 different persons")
                    if(hours>0.3):
                        
                        right_box.write("You have passed all requirements - initiating transfer ")
                        '''
                        nodes = ["https://anyx.io/", "https://hive.roelandp.nl","rpc.ausbit.dev"]

                        active_key=os.environ['ACTIVE']
                        json_object = {
                        "contractName":"tokens",
                        "contractAction":"transfer",
                        "contractPayload": {
                        "symbol":sym,
                        "to": user,
                        "quantity": str(quantity),
                        "memo": "From HCS - rewards for engagement"}}


                        idxxx = "ssc-mainnet-hive"
                        stm = Hive(nodes)
                        set_shared_steem_instance(stm)
                        tx = TransactionBuilder(steem_instance=stm)
                        op = operations.Custom_json(** {
                                                    "required_auths": ["amr008.rewards"],
                                                    "required_posting_auths": [],
                                                    "id": idxxx,
                                                    "json": json_object
                                                } )
                        tx.appendOps([op])
                        tx.appendWif(active_key) 
                        sign_tx=tx.sign()
                        tx_b=tx.broadcast()
                        print("1")
                        print("SENT to {}: {} , Tx id :{}".format(user,quantity,sign_tx.id))
                        
                        '''
                        df_store_csv=pd.DataFrame([[user,quantity,dt.utcnow().date(),sym]])
                        df_store_csv.to_csv('user_claim.csv',mode='a',index=False,header=False)
                        

                        time.sleep(3)
                    else:
                        right_box.write("You seem to have rushed through and made comments in a hurry , you have to slow down , take your time and come back")
                        
                else:
                    right_box.write("You have talked to less than 5 people , please talk to atleast 5 people and come back ")
                    
            else:
                right_box.write("Poor quality , put more effort and come back - What can you do ? Post more quality comments on user posts")
                
        else:
            right_box.write("Less than 10 comments made ,make atleast 10 comments from {} front-end and come back".format(frontend))
            

    else:
        right_box.write("You already claimed {} today, come back tomorrow".format(sym))
        
        
if __name__ == '__main__':

    st.set_page_config(page_title='Hive Community stats, Earn rewards for engagement',layout='wide')
    
    uid = os.environ['UID']
    pwd = os.environ['PWD']
    uid=str(uid)
    pwd=str(pwd)

    conn = establish_connection(uid,pwd)
    
    user=st.text_input("Enter username: ")
    sym=st.selectbox("Select token: ",['LEO','SPORTS','CTP']) 
    sym=sym.upper()

    if user:
        if sym:
            st.write("okay")
            left_box,right_box= st.beta_columns([1,3])
            
            frontend=get_frontend(sym)
            
            comment_query,post_query = retrieve_query(user,conn)
            
            number_of_comments,sum_len,hours,authors_talked_to = retrieve_data(comment_query,post_query,frontend)
            
            quantity = calculation(number_of_comments,sum_len,hours,authors_talked_to,sym)
            
            transaction_check(user,number_of_comments,sum_len,authors_talked_to,hours,quantity,sym)
    

    
    
            

