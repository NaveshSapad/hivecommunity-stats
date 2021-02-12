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
from hiveengine.api import Api
import requests
import pandas as pd
import pymssql
import json
from datetime import datetime as dt
from datetime import timedelta
import time
import os
import streamlit as st


def establish_connection(uid,pwd):

    conn = pymssql.connect(server='vip.hivesql.io', user=uid, password=pwd, database='DBHive')

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

def todays_data(comment_query,post_query):
    front_end_list_c=[]
    front_end_list_p=[]
    
    for i in range(0,len(comment_query)):
        json_fe=json.loads(comment_query['json_metadata'][i])
        if 'app' in json_fe:
            front_end_list_c.append([json_fe['app'],1])

    for i in range(0,len(post_query)):
        json_fe=json.loads(post_query['json_metadata'][i])
        if 'app' in json_fe:
            front_end_list_p.append([json_fe['app'],1])

    if front_end_list_c:
        df_c=pd.DataFrame(front_end_list_c)
        df_c.columns=['frontend','count']
    else:
        df_c=pd.DataFrame(columns=['frontend','count'])

    if front_end_list_p:
        df_p=pd.DataFrame(front_end_list_p)
        df_p.columns=['frontend','count']
    else:
        df_p=pd.DataFrame(columns=['frontend','count'])

    #df_c_c=pd.DataFrame(columns=['count'])
    #df_p_c=pd.DataFrame(columns=['count'])

    #df_c_c['count']=df_c.count()
    #df_p_c['count']=df_p.count()


    
    right_posts.table(df_p.groupby('frontend').sum())
    right_columns.table(df_c.groupby('frontend').sum())
    
    

def retrieve_data(comment_query,post_query,frontend):
        
    
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

    
    todays_data(comment_query,post_query)
    
    
    for i in range(0,len(comment_query)):
        if(json.loads(comment_query['json_metadata'][i])['app'].startswith(frontend)): # frontend
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
    
    return number_of_comments,sum_len,hours,authors_talked_to,number_p,number_c

def calculation(number_of_comments,sum_len,hours,authors_talked_to,sym):
    
    quantity_p = (authors_talked_to * 0.005) + (sum_len * 0.0001) + (number_of_comments * 0.0075)


    if(quantity_p>1):
        quantity_p=1


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

def check_claim(sym):
    s=[]
    end=0
    x=0
    while(end!=1):
        res=api.get_history('amr008.rewards',sym,offset=x)
        s.append(res)

        x=x+len(res)
        if(len(res)<500):
            end=1


    listfinal=[]
    for i in range(0,len(s)):
        for j in range(0,len(s[i])):
            listfinal.append(s[i][j])

    all_list=[]
    user_list=[]
    for i in range(0,len(listfinal)):
        if(listfinal[i]['from']=='amr008.rewards'):
            if(time.strftime('%Y-%m-%d', time.localtime(listfinal[i]['timestamp']))==str(dt.utcnow().date())):
                user_list.append(listfinal[i]['to'])

    return user_list

def transaction_check(user,number_of_comments,sum_len,authors_talked_to,hours,quantity,sym,frontend):
    user_list=check_claim(sym)
    
    
    if user not in user_list:
        if(number_of_comments>=10):
            right_box.markdown("<p class='positive'><b>Number of comments satisfied</b>,<b>Comments made from {} :</b> <i>{}</i></p>".format(frontend,str(number_of_comments)),unsafe_allow_html=True)
            time.sleep(1)
            if(sum_len>100):
                right_box.write("<p class='positive'><b>Quality of comments satisfied</b></p>",unsafe_allow_html=True)
                time.sleep(1)
                if(authors_talked_to>2):
                    right_box.write("<p class='positive'><b>You have talked to atleast 5 different persons</b>,<b>Authors talked to:</b> <i>{}</i></p>".format(str(authors_talked_to)),unsafe_allow_html=True)
                    time.sleep(1)
                    if(hours>0.3):
                        
                        right_box.write("<p class='positive'><b>You have passed all requirements - initiating transfer </b></p>",unsafe_allow_html=True)

                        if right_box.write("Click here to claim now"):
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

                            right_box.write("<p class='positive'>SENT to {}: {} {}, Tx id :{}</p>".format(user,quantity,sym,sign_tx.id),unsafe_allow_html=True)
                        

                        #df_store_csv=pd.DataFrame([[user,quantity,dt.utcnow().date(),sym]])
                        #df_store_csv.to_csv('user_claim.csv',mode='a',index=False,header=False)
                        

                            time.sleep(3)

                    else:
                        right_box.markdown("<p class='negative'><b>You seem to have rushed through and made comments in a hurry , you have to slow down , take your time and come back</b></p>",unsafe_allow_html=True)
                            
                else:
                    right_box.markdown("<p class='negative'><b>Please talk to atleast 5 people and come back</b>,<b>Number of authors talked to so far :</b> <i>{}</i></p> ".format(str(authors_talked_to)),unsafe_allow_html=True)
                        
            else:
                right_box.markdown("<p class='negative'><b>Poor quality , put more effort and come back - What can you do ? Post more quality comments on user posts</b></p>",unsafe_allow_html=True)
                    
        else:
            right_box.markdown("<p class='negative'><b>Less than 10 comments made ,make atleast 10 comments from {} front-end and come back</b>,<b>Comments made :</b> <i>{}</i></p>".format(frontend,str(number_of_comments)),unsafe_allow_html=True)
                
    else:
        right_box.markdown("<p class='negative'><b>You already claimed {} today, come back tomorrow</b></p>".format(sym),unsafe_allow_html=True)


def left_bottom_data(number_p,number_c):

    data_left_b.markdown("<h2> Number of posts from all front-end : {} </h2><h2> Number of comments from all front-end : {} </h2> ".format(number_p,number_c),unsafe_allow_html=True)

        
if __name__ == '__main__':

    st.set_page_config(page_title='Hive Community stats, Earn rewards for engagement',layout='wide')

    api=Api()
    
    uid = os.environ['hiveuid']
    pwd = os.environ['hivepwd']

    st.markdown('''<style>
                    body { background-color:#000;
                    }
                    #title {color:#ffffff;}
                    
                    
                    .css-wctngo {background-color:#ffffff;
                    padding:25px;
                    border-radius:10px;}
                    
                    .css-1oyz2rp  {background-color:#ffffff;
                    padding:25px;
                    border-radius:10px;}

                    .css-t1ghz5 {
                    
                    background-color:#ffffff;
                    padding:25px;
                    margin-top:50px;
                    border-radius:10px;}

                    .css-1hgnhsa {background-color:#ffffff;
                    padding:25px;
                    margin-top:50px;
                    border-radius:10px;}

                    .negative {color:red; font-size:22px;}
                    .positive {color:green;font-size:22px;}
                    
                    #date{color:#ffffff}

                    
                    </style>''',unsafe_allow_html=True)


    st.markdown("<h1 id='title'><center>Hive Community Stats -You get paid for your engagement</center></h1>",unsafe_allow_html=True)


    conn = establish_connection(uid,pwd)

    left_upper,right_box=st.beta_columns([1,4])

    left_bottom,right_posts,right_columns=st.beta_columns([2,2,2])

    st.markdown("<p id='date'><center> {} </center></p>".format(str(dt.utcnow())),unsafe_allow_html=True)

    left_bottom.markdown("<h1>Your data for today : {} </h1>".format(dt.utcnow().date()),unsafe_allow_html=True)
    right_posts.markdown("<h1>Your Posts data for today </h1>",unsafe_allow_html=True)
    right_columns.markdown("<h1>Your Comments data for today  </h1>",unsafe_allow_html=True)

    data_left_b=left_bottom.empty()
    data_left_b.write("Click claim rewards button to retrieve data")
    
    user=left_upper.text_input("Enter username: ")
    sym=left_upper.selectbox("Select token: ",['LEO','SPORTS','CTP']) 
    sym=sym.upper()

    frontend=get_frontend(sym)
    right_box.markdown("<h1><center>Your {} rewards criteria check </center></h1>".format(sym),unsafe_allow_html=True)

    if left_upper.button("Get my data and claim rewards"):
        if user:
            if sym:                
                
                comment_query,post_query = retrieve_query(user,conn)
                
                number_of_comments,sum_len,hours,authors_talked_to,number_p,number_c = retrieve_data(comment_query,post_query,frontend)


                left_bottom_data(number_p,number_c)
                               
                
                quantity = calculation(number_of_comments,sum_len,hours,authors_talked_to,sym)
                
                transaction_check(user,number_of_comments,sum_len,authors_talked_to,hours,quantity,sym,frontend)
            else:
                left_upper.write("Select symbol")
        else:
            left_upper.write("Enter username")

            
    

    
    
            

