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
import altair as alt
import streamlit as st
import matplotlib.pyplot as plt
import datetime



def establish_conn(uid,pwd):
    conn = pymssql.connect(server='vip.hivesql.io', user=uid, password=pwd, database='DBHive')
    
    return conn

if __name__ == '__main__':

    st.set_page_config(page_title='Hive Community Stats',layout='wide')
    
    uid = os.environ['hiveuid']
    pwd = os.environ['hivepwd']

    conn=establish_conn(uid,pwd)

    st.markdown('''<style>
    .css-z8kais{align-items:center;}
    .css-1bjf9av .css-106gl43 {border:3px black solid;}

    .css-1f5jof3{margin-left:100px;}
    
    </style>    ''',unsafe_allow_html=True)

    user=st.sidebar.text_input("Enter your Hive username")
    user=user.lower()
    title=st.empty()

    p_d = st.sidebar.empty()
    p_text = st.sidebar.progress(0)
    
    
    title.markdown("<h1><center> Enter your Hive username in the sidebar to get your data ( left) </center></h1>",unsafe_allow_html=True)
    if user:
       
        title.empty()
        
        p_text.progress(20)
        p_d.write("Retrieving your info")
        
        user_pc = pd.read_sql_query('''select * from Comments where author= '{}' and created > GETDATE()-31  ORDER BY ID DESC '''.format(user),conn)

        p_d.write("Data received , preparing charts")
        p_text.progress(60)
        
        post_c=0
        comment_c=0
        save_list=[]
        for i in range(0,len(user_pc)):
            try:
                json_metadata=json.loads(user_pc['json_metadata'][i])
                if 'app' in json_metadata:
                    save_list.append([user_pc['author'][i],user_pc['parent_author'][i],json_metadata['app'],user_pc['created'][i].date()])
                    
                    if(user_pc['parent_author'][i]==''):
                        post_c += 1
                    else:
                        comment_c += 1
                        
            except:
                print("Yes")
                pass

        p_text.progress(80)
        
            
        st.markdown("<h3><center> Your total post count for past 30 days: {}, Your total comment count for past 30 days: {}</center></h3><hr>".format(post_c,comment_c),unsafe_allow_html=True)
            
        df_pc = pd.DataFrame(save_list,columns=['Author','Parent_author','Frontend','Date'])

        

        df_pc['Type']=str
        for i in range(0,len(df_pc)):
            if df_pc['Parent_author'][i]=='':
                df_pc['Type'][i]='Post'
            else:
                df_pc['Type'][i]='Comment'

        df_today_pc = df_pc[df_pc['Date']==dt.utcnow().date()]
        df_today_pie= df_today_pc.groupby('Frontend').count() 

        df_p=df_pc[df_pc['Parent_author']=='']
        df_c=df_pc[df_pc['Parent_author']!='']
        
        df_today_p= df_p[df_p['Date']==dt.utcnow().date()]
        df_today_frontends_p=df_today_p.groupby('Frontend').count()

        df_today_c= df_c[df_c['Date']==dt.utcnow().date()]
        df_today_frontends_c=df_today_c.groupby('Frontend').count()

        df_today_frontends_p.rename(columns={'Author':'Post_count'},inplace=True)
        df_today_frontends_c.rename(columns={'Author':'Comment_count'},inplace=True)
        
        

        base=alt.Chart(df_pc)
        
        p_text.progress(75)
        
        st.markdown("<h1><center> Post + Comment count Date wise </center></h1>",unsafe_allow_html=True)
        
        b = base.mark_bar(size=20,point=True).encode(x='Date', y='count(Author)',color='Type',tooltip=['count(Type)','Type','Date']).configure_axis(
        labelFontSize=20,
        titleFontSize=20
    ).properties(width=1000,height=700)

        st.write(b)

        base1=alt.Chart(df_pc)

        st.markdown("<hr><h1> <center> Frontend Data </center> </h1>",unsafe_allow_html=True)

        c= base1.mark_bar(size=20,point=True).encode(x='Date',y='count(Author)',color='Frontend',tooltip=['count(Frontend)','Frontend']).configure_axis(
        labelFontSize=20,
        titleFontSize=20
    ).properties(width=1000,height=700)

        st.write(c)

        st.markdown("<h1><center> Today's data </center></h1>",unsafe_allow_html=True)

        left_today,right_today= st.beta_columns([1,1])

        left_today.write("<h3><center> Posts </center></h3>",unsafe_allow_html=True)

        right_today.write("<h3> <center> Comments </center> </h3>",unsafe_allow_html=True)

        

        left_today.table(df_today_frontends_p['Post_count'])
        right_today.table(df_today_frontends_c['Comment_count'])

        p_text.progress(100)
        p_d.write("Data displayed")

    
    st.markdown("<hr><h1> <center> Engagement Program </center> </h1>",unsafe_allow_html=True)

    d = st.date_input(
         "Choose the date to display the data",
         datetime.date(2021, 2, 16),
         min_value=datetime.date(2021, 2, 15),
         max_value=datetime.date(2021, 2, 16))

    st.write('Selected Date:', str(d))

    engage_leo,engage_ctp = st.beta_columns(2)
    engage_stem,engage_sports = st.beta_columns(2)


    if d:
        file_name='Images_{}'.format(str(d))

        engage_leo.markdown("<h3> LeoFinance Engagement for {} </h3>".format(str(d)),unsafe_allow_html=True)
        engage_leo.image(file_name+'/leo.png')
        
        engage_ctp.markdown("<h3>Ctptalk Engagement for {} </h3>".format(str(d)),unsafe_allow_html=True)
        engage_ctp.image(file_name+'/ctp.png')
        
        engage_stem.markdown("<hr><h3> STEMGeeks Engagement for {} </h3>".format(str(d)),unsafe_allow_html=True)
        engage_stem.image(file_name+'/stem.png')
        
        engage_sports.markdown("<hr><h3> Sportstalksocial Engagement for {} </h3>".format(str(d)),unsafe_allow_html=True)
        engage_sports.image(file_name+'/sports.png')

    
        
        

       
    
        

        
