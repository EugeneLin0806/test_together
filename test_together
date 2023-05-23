import asyncio
import socket
from flask import Flask, render_template, request, url_for, redirect, Response, send_from_directory, make_response, \
    jsonify

from werkzeug.utils import secure_filename

from flask_bootstrap import Bootstrap

from flask_mail import Mail, Message

from threading import Thread

import os
import re
import logging
import pymssql
import time
import requests
import json
import datetime
import smtplib
import random
import email
import subprocess
from email.mime.text import MIMEText
from email.header import Header

count = 0
Update_Cascade_EmailScheduleState = []
cont_errorEmail = 0
PID_IsRepeat = []
p = re.compile(r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)") #正則運算Email
recipient_file = open('/home/survey/sql_file/SQL_config.txt', 'r')
for ss in recipient_file:
    line_list = re.split(",", ss)
    if "gsdb01p_ap" in line_list:
        which_AWS = "mastar"
    elif "gsdb01t_ap" in line_list:
        which_AWS = "Concat_UAT"

def mssql_conn():
    recipient_file = open('/home/survey/sql_file/SQL_config.txt', 'r')
    for ss in recipient_file:
        line_list = re.split(",", ss)
    MSSQL_CONN = pymssql.connect(host=line_list[0], server='SZS\SQLEXPRESS', port=line_list[1], user=line_list[2],
                                 password=line_list[3], database=line_list[4])
    return MSSQL_CONN


def check_host_availability(host, port):
    try:
        with smtplib.SMTP(host, port, timeout=5):
            return True
    except (socket.timeout, ConnectionRefusedError):
        return False


def send_async_email(Cascade_Project_Subject, sender, PID, CustomPID, Name, Cascade_EmailScheduleState_No, User_email, Cascade_Project_No, Cascade_Project_Number, Cascade_Gift, Cascade_Project_Start_Time, Cascade_Project_End_Time, Cascade_Email_data, Cascade_Project_Email_Subject, Cascade_Send_Data_No, Cascade_CustomPID_List_No, which_task):  # Seq_No是更新Sender,Sender_Group是更新Sender_Volume
    global count
    global Update_Cascade_EmailScheduleState
    global which_AWS
    global cont_errorEmail

    # socks.setdefaultproxy(TYPE, ADDR, PORT)
    # socks.setdefaultproxy(socks.SOCKS5, 'proxy.proxy.com', 8080)
    # socks.wrapmodule(smtplib)

    # 第三方 SMTP 服务
    # mail_host = "10.10.18.41"  # 设置服务器
    # mail_host = "10.10.18.42"  # 设置服务器
    # mail_host = "10.10.18.43"  # 设置服务器
    # mail_host = "10.10.18.44"  # 设置服务器
    mail_host = ""
    if which_task == "task1":
        # host_list = ["10.97.2.123", "10.10.13.20"]
        host_list = ["10.10.17.41", "10.10.17.42"]
        port = 25
        for host in host_list:
            if check_host_availability(host, port):
                print(f'{host}:{port} is reachable.')
            else:
                print(f'{host}:{port} is not reachable.')
                host_list.remove(host)
        print(host_list)
        if len(host_list) == 0:
            print("task1切換成10.10.13.2", "10.10.13.22")
            # host_list = ["10.10.13.2", "10.10.13.22"]
            host_list = ["10.10.17.43", "10.10.17.44"]
            port = 25
            for host in host_list:
                if check_host_availability(host, port):
                    print(f'{host}:{port} is reachable.')
                else:
                    print(f'{host}:{port} is not reachable.')
        mail_host = random.choice(host_list)
    elif which_task == "task2":
        # host_list = ["10.10.13.2", "10.10.13.22"]
        host_list = ["10.10.18.41", "10.10.18.42"]
        port = 25
        for host in host_list:
            if check_host_availability(host, port):
                print(f'{host}:{port} is reachable.')
            else:
                print(f'{host}:{port} is not reachable.')
                host_list.remove(host)
        print(host_list)
        if len(host_list) == 0:
            print("task2切換成10.97.2.123", "10.10.13.20")
            # host_list = ["10.97.2.123", "10.10.13.20"]
            host_list = ["10.10.18.43", "10.10.18.44"]
            port = 25
            for host in host_list:
                if check_host_availability(host, port):
                    print(f'{host}:{port} is reachable.')
                else:
                    print(f'{host}:{port} is not reachable.')
        mail_host = random.choice(host_list)


    # mail_user = "gosurvey@gosurvey.com.tw"  # 用户名
    # mail_pass = "XXXXXX"  # 密碼
    print(User_email)
    sender = 'gosurvey@gosurvey.com.tw'
    receivers = [User_email]  # 接收邮件
    html_body = str(Cascade_Email_data[0][2])
    if which_AWS == "mastar":
        html_body = html_body.replace('{{which_AWS}}', "m")
    if which_AWS == "Concat_UAT":
        html_body = html_body.replace('{{which_AWS}}', "uat")

    html_body = html_body.replace('{{Custom_PID}}', str(CustomPID))
    html_body = html_body.replace('{{Cascade_EmailScheduleState_No}}', str(Cascade_EmailScheduleState_No))
    html_body = html_body.replace('{{Cascade_Project_No}}', str(Cascade_Project_No))
    if which_AWS == "mastar":
        html_body = html_body.replace('{{url}}', 'https://event.gosurvey.com.tw/Cascade_import/Survey/m/' + str(
            CustomPID) + "/" + str(Cascade_EmailScheduleState_No) + "/" + str(Cascade_Project_No))
    if which_AWS == "Concat_UAT":
        html_body = html_body.replace('{{url}}', 'https://event.gosurvey.com.tw/Cascade_import/Survey/uat/' + str(
            CustomPID) + "/" + str(Cascade_EmailScheduleState_No) + "/" + str(Cascade_Project_No))

    html_body = html_body.replace('{{name}}', Name)
    html_body = html_body.replace('{{project_number}}', Cascade_Project_Number)
    print(Cascade_Project_Number)
    html_body = html_body.replace('{{project_subject}}', Cascade_Project_Subject)
    print(Cascade_Project_Subject)
    html_body = html_body.replace('{{gift}}', Cascade_Gift)
    print(Cascade_Gift)
    html_body = html_body.replace('{{Begin_and_End_Date}}', Cascade_Project_Start_Time[0:4] + " 年 " + Cascade_Project_Start_Time[5:7] + " 月 " + Cascade_Project_Start_Time[8:10] + " 日 至 " + Cascade_Project_End_Time[5:7] + " 月 " + Cascade_Project_End_Time[8:10] + " 日 止")
    print(Cascade_Project_Start_Time + Cascade_Project_End_Time)
    print(mail_host)
    message = MIMEText(html_body, 'html', 'utf-8')
    message['From'] = Header('GO SURVEY <gosurvey@gosurvey.com.tw>')
    message['To'] = User_email
    message['Date'] = email.utils.formatdate()
    message['Message-ID'] = email.utils.make_msgid(domain='gosurvey.com.tw')

    subject = Cascade_Project_Email_Subject
    message['Subject'] = Header(subject, 'utf-8')

    try:
        smtpObj = smtplib.SMTP()
        smtpObj.connect(mail_host, 25)  # 25 为 SMTP 端口号
        # smtpObj.login(mail_user, mail_pass)
        smtpObj.sendmail(sender, receivers, str(message).encode('utf-8'))
        print("郵件發送成功")
        Update_Cascade_EmailScheduleState.append(Cascade_EmailScheduleState_No)
        count = count + 1
        smtpObj.close()
    except Exception as e:
        print("Error: 郵件發送邮件")
        print(e)
        MYSQL_CONN = mssql_conn()
        cur = MYSQL_CONN.cursor()

        insert_User_Info = """
                                UPDATE Cascade_EmailScheduleState SET Cascade_Send_Data_Status = '6', SendingStatus = '6' WHERE Cascade_Send_Data_No = %s and Cascade_Project_No = %s and Cascade_EmailScheduleState_No = %s
                                                                                                                                       """
        cur.execute(insert_User_Info, (Cascade_Send_Data_No, Cascade_Project_No, Cascade_EmailScheduleState_No))

        MYSQL_CONN.commit()
        cur.close()

        cont_errorEmail = cont_errorEmail + 1


async def task1(Cascade_Project_No, Cascade_Send_Data_No):
    global PID_IsRepeat
    print("Task 1 is running...")
    print(Cascade_Send_Data_No)

    # 查出Cascade_Project_No的資料
    MYSQL_CONN = mssql_conn()
    cur = MYSQL_CONN.cursor()

    Cascade_Project_Info = """
                                Select Cascade_Project_Subject, Cascade_Project_Link, Cascade_Project_Number, Cascade_Project_Start_Time, Cascade_Project_End_Time, Cascade_Project_Email_Subject From Cascade_Project where Cascade_Project_No = %s 
                                                                                        """
    cur.execute(Cascade_Project_Info, (Cascade_Project_No))
    Cascade_Project_Data = cur.fetchone()
    MYSQL_CONN.commit()
    cur.close()

    Cascade_Project_Subject = Cascade_Project_Data[0]
    Cascade_Project_Link = Cascade_Project_Data[1]
    Cascade_Project_Number = Cascade_Project_Data[2]
    Cascade_Project_Start_Time = Cascade_Project_Data[3]
    Cascade_Project_End_Time = Cascade_Project_Data[4]
    Cascade_Project_Email_Subject = Cascade_Project_Data[5]

    # 查出Cascade_Gift_List裡的Gift的資料
    MYSQL_CONN = mssql_conn()
    cur = MYSQL_CONN.cursor()

    Cascade_Project_Info = """
                        SELECT Cascade_Gift FROM dbo.Cascade_Gift_List where Cascade_Send_Data_No = %s                                                                                            """
    cur.execute(Cascade_Project_Info, (Cascade_Send_Data_No))
    Cascade_Gift_List_Data = cur.fetchone()
    MYSQL_CONN.commit()
    cur.close()

    Cascade_Gift = Cascade_Gift_List_Data[0]

    # 查出這筆Cascade_Send_Data的全部ＣＭＳ資料(不包含黑名單的)
    MYSQL_CONN = mssql_conn()
    cur = MYSQL_CONN.cursor()

    insert_User_Info = """
                                Select a.Cascade_Project_No, a.Cascade_CMS_List_No, a.Cascade_Send_Data_No, a.Cascade_EmailScheduleState_PID, b.Cascade_CMS_List_EMail, b.Cascade_CMS_List_Name, a.Cascade_EmailScheduleState_No, c.Cascade_CustomPID, c.Cascade_CustomPID_List_No
                                from Cascade_EmailScheduleState a 
                                left join Cascade_CMS_List b on a.Cascade_EmailScheduleState_PID = b.Cascade_CMS_List_PID and a.Cascade_CMS_List_No = b.Cascade_CMS_List_No 
                                left join Cascade_CustomPID_List c on c.Cascade_CustomPID_List_No = a.Cascade_CustomPID_List_No 
                                where a.Cascade_Project_No = %s  and a.Cascade_Send_Data_No = %s and a.SendingStatus in (0) and c.Cascade_CustomPID_List_No is not Null
                                and a.Shunt_No = 0 and b.Cascade_CMS_List_EMail not in (
                                    select EMail from Black_List where EMail = b.Cascade_CMS_List_EMail 
                                )
                                                                                        """
    cur.execute(insert_User_Info, (Cascade_Project_No, Cascade_Send_Data_No))
    Cascade_CMS_List_Data = cur.fetchall()
    MYSQL_CONN.commit()
    cur.close()

    # 查出這筆Cascade_Email的資料
    MYSQL_CONN = mssql_conn()
    cur = MYSQL_CONN.cursor()

    query_script = """
                                select * from Cascade_Email where Cascade_Project_No = %s 

                                                                       """
    cur.execute(query_script, (Cascade_Project_No))
    Cascade_Email_data = cur.fetchall()
    cur.close()

    # 查要發送名單裡的所有黑名單
    sql3_ = """Select a.Cascade_Project_No, a.Cascade_CMS_List_No, a.Cascade_Send_Data_No, a.Cascade_EmailScheduleState_PID, b.Cascade_CMS_List_EMail, b.Cascade_CMS_List_Name, a.Cascade_EmailScheduleState_No, c.Cascade_CustomPID, c.Cascade_CustomPID_List_No
                                from Cascade_EmailScheduleState a 
                                left join Cascade_CMS_List b on a.Cascade_EmailScheduleState_PID = b.Cascade_CMS_List_PID and a.Cascade_CMS_List_No = b.Cascade_CMS_List_No 
                                left join Cascade_CustomPID_List c on c.Cascade_CustomPID_List_No = a.Cascade_CustomPID_List_No 
                                where a.Cascade_Project_No = %s  and a.Cascade_Send_Data_No = %s and a.SendingStatus in (0) and c.Cascade_CustomPID_List_No is not Null
                                and a.Shunt_No = 0 and b.Cascade_CMS_List_EMail in (
                                select EMail from  Black_List where EMail = b.Cascade_CMS_List_EMail
                                )"""
    print(sql3_)
    MYSQL_CONN = mssql_conn()
    cur = MYSQL_CONN.cursor()
    cur.execute(sql3_, (Cascade_Project_No, Cascade_Send_Data_No))
    select_EmailBlock_exist_data = cur.fetchall()

    Update_EmailBlock = []
    for EmailBlock in select_EmailBlock_exist_data:
        cont_errorEmail = cont_errorEmail + 1
        Update_EmailBlock.append((EmailBlock[2], EmailBlock[0], EmailBlock[6]))

    MYSQL_CONN = mssql_conn()
    cur = MYSQL_CONN.cursor()

    insert_User_Info = """
                                 UPDATE Cascade_EmailScheduleState SET Cascade_Send_Data_Status = '6', SendingStatus = '6' WHERE Cascade_Send_Data_No = %s and Cascade_Project_No = %s and Cascade_EmailScheduleState_No = %s
                                                                                                                                   """
    cur.executemany(insert_User_Info, Update_EmailBlock)

    MYSQL_CONN.commit()
    cur.close()

    fin_item = []
    Update_Cascade_CustomPID_List = []
    Chaeck_Email_Repeat = []
    Chaeck_PID_Repeat = []

    for idx in range(0, len(Cascade_CMS_List_Data)):
        print(Cascade_CMS_List_Data[idx])
        print(Cascade_CMS_List_Data[idx])
        Project_No = Cascade_CMS_List_Data[idx][0]
        Cascade_CMS_List_No = Cascade_CMS_List_Data[idx][1]
        PID = Cascade_CMS_List_Data[idx][3]
        User_email = Cascade_CMS_List_Data[idx][4]
        Name = Cascade_CMS_List_Data[idx][5]
        Cascade_EmailScheduleState_No = Cascade_CMS_List_Data[idx][6]
        CustomPID = Cascade_CMS_List_Data[idx][7]
        Cascade_CustomPID_List_No = Cascade_CMS_List_Data[idx][8]
        print(User_email, PID, CustomPID)
        print(Cascade_Project_Subject)
        print(Cascade_Project_Link)

        # 讓重複Email不發送
        if User_email in Chaeck_Email_Repeat:
            pass
        else:
            # 讓重複PID不發送
            if PID in Chaeck_PID_Repeat:
                PID_IsRepeat.append((PID, Cascade_CMS_List_No))
                pass
            else:
                sender = ['GO SURVEY', 'service@dm.gosurvey.com.tw']
                AA = re.split("@", User_email)
                check_email = re.split("@", User_email)
                if User_email == User_email.replace(' ', '') and AA[0] != "":
                    if User_email == User_email.replace(' ', '') and check_email[0] != "" and len(check_email) == 2:
                        if not p.match(User_email):
                            continue
                        else:
                            send_async_email(Cascade_Project_Subject, sender, PID, CustomPID, Name,
                                             Cascade_EmailScheduleState_No, User_email, Cascade_Project_No,
                                             Cascade_Project_Number, Cascade_Gift, Cascade_Project_Start_Time,
                                             Cascade_Project_End_Time, Cascade_Email_data,
                                             Cascade_Project_Email_Subject, Cascade_Send_Data_No,
                                             Cascade_CustomPID_List_No, 'task1')
                            Chaeck_Email_Repeat.append(User_email)
                            Chaeck_PID_Repeat.append(PID)
                            # # 查出這筆Cascade_CustomPID_List有無被寫入的資料
                            # MYSQL_CONN = mssql_conn()
                            # cur = MYSQL_CONN.cursor()
                            #
                            # query_script = """
                            #                             select * from Cascade_CustomPID_List where Cascade_Project_No = %s and Cascade_CustomPID = %s and Cascade_CMS_List_No = %s and Cascade_EmailScheduleState_No = %s
                            #
                            #                                                                    """
                            # cur.execute(query_script, (Project_No, CustomPID, Cascade_CMS_List_No, Cascade_EmailScheduleState_No))
                            # Cascade_CustomPID_List_exist_data = cur.fetchall()
                            # cur.close()
                            #
                            # if Cascade_CustomPID_List_exist_data is not None:
                            #     Update_Cascade_CustomPID_List.append((Cascade_CMS_List_No, Cascade_EmailScheduleState_No, Project_No, CustomPID))
                            # else:
                            #     pass
                    else:
                        MYSQL_CONN = mssql_conn()
                        cur = MYSQL_CONN.cursor()

                        insert_User_Info = """
                                                        UPDATE Cascade_EmailScheduleState SET Cascade_Send_Data_Status = '6', SendingStatus = '6' WHERE Cascade_Send_Data_No = %s and Cascade_Project_No = %s and Cascade_EmailScheduleState_No = %s
                                                                                                                                   """
                        cur.execute(insert_User_Info,
                                    (Cascade_Send_Data_No, Cascade_Project_No, Cascade_EmailScheduleState_No))

                        MYSQL_CONN.commit()
                        cur.close()

                        cont_errorEmail = cont_errorEmail + 1

    await asyncio.sleep(0.1)


async def task2(Cascade_Project_No, Cascade_Send_Data_No):
    global PID_IsRepeat
    print("Task 2 is running...")
    print(Cascade_Send_Data_No)


    # 查出Cascade_Project_No的資料
    MYSQL_CONN = mssql_conn()
    cur = MYSQL_CONN.cursor()

    Cascade_Project_Info = """
                                Select Cascade_Project_Subject, Cascade_Project_Link, Cascade_Project_Number, Cascade_Project_Start_Time, Cascade_Project_End_Time, Cascade_Project_Email_Subject From Cascade_Project where Cascade_Project_No = %s 
                                                                                        """
    cur.execute(Cascade_Project_Info, (Cascade_Project_No))
    Cascade_Project_Data = cur.fetchone()
    MYSQL_CONN.commit()
    cur.close()

    Cascade_Project_Subject = Cascade_Project_Data[0]
    Cascade_Project_Link = Cascade_Project_Data[1]
    Cascade_Project_Number = Cascade_Project_Data[2]
    Cascade_Project_Start_Time = Cascade_Project_Data[3]
    Cascade_Project_End_Time = Cascade_Project_Data[4]
    Cascade_Project_Email_Subject = Cascade_Project_Data[5]

    # 查出Cascade_Gift_List裡的Gift的資料
    MYSQL_CONN = mssql_conn()
    cur = MYSQL_CONN.cursor()

    Cascade_Project_Info = """
                        SELECT Cascade_Gift FROM dbo.Cascade_Gift_List where Cascade_Send_Data_No = %s                                                                                            """
    cur.execute(Cascade_Project_Info, (Cascade_Send_Data_No))
    Cascade_Gift_List_Data = cur.fetchone()
    MYSQL_CONN.commit()
    cur.close()

    Cascade_Gift = Cascade_Gift_List_Data[0]

    # 查出這筆Cascade_Send_Data的全部ＣＭＳ資料(不包含黑名單的)
    MYSQL_CONN = mssql_conn()
    cur = MYSQL_CONN.cursor()

    insert_User_Info = """
                                Select a.Cascade_Project_No, a.Cascade_CMS_List_No, a.Cascade_Send_Data_No, a.Cascade_EmailScheduleState_PID, b.Cascade_CMS_List_EMail, b.Cascade_CMS_List_Name, a.Cascade_EmailScheduleState_No, c.Cascade_CustomPID, c.Cascade_CustomPID_List_No
                                from Cascade_EmailScheduleState a 
                                left join Cascade_CMS_List b on a.Cascade_EmailScheduleState_PID = b.Cascade_CMS_List_PID and a.Cascade_CMS_List_No = b.Cascade_CMS_List_No 
                                left join Cascade_CustomPID_List c on c.Cascade_CustomPID_List_No = a.Cascade_CustomPID_List_No 
                                where a.Cascade_Project_No = %s  and a.Cascade_Send_Data_No = %s and a.SendingStatus in (0) and c.Cascade_CustomPID_List_No is not Null
                                and a.Shunt_No = 1 and b.Cascade_CMS_List_EMail not in (
                                select EMail from  Black_List where EMail = b.Cascade_CMS_List_EMail 
                                )
                                                                                        """
    cur.execute(insert_User_Info, (Cascade_Project_No, Cascade_Send_Data_No))
    Cascade_CMS_List_Data = cur.fetchall()
    MYSQL_CONN.commit()
    cur.close()

    # 查出這筆Cascade_Email的資料
    MYSQL_CONN = mssql_conn()
    cur = MYSQL_CONN.cursor()

    query_script = """
                                select * from Cascade_Email where Cascade_Project_No = %s 

                                                                       """
    cur.execute(query_script, (Cascade_Project_No))
    Cascade_Email_data = cur.fetchall()
    cur.close()

    # 查要發送名單裡的所有黑名單
    sql3_ = """Select a.Cascade_Project_No, a.Cascade_CMS_List_No, a.Cascade_Send_Data_No, a.Cascade_EmailScheduleState_PID, b.Cascade_CMS_List_EMail, b.Cascade_CMS_List_Name, a.Cascade_EmailScheduleState_No, c.Cascade_CustomPID, c.Cascade_CustomPID_List_No
                                from Cascade_EmailScheduleState a 
                                left join Cascade_CMS_List b on a.Cascade_EmailScheduleState_PID = b.Cascade_CMS_List_PID and a.Cascade_CMS_List_No = b.Cascade_CMS_List_No 
                                left join Cascade_CustomPID_List c on c.Cascade_CustomPID_List_No = a.Cascade_CustomPID_List_No 
                                where a.Cascade_Project_No = %s  and a.Cascade_Send_Data_No = %s and a.SendingStatus in (0) and c.Cascade_CustomPID_List_No is not Null
                                and a.Shunt_No = 1 and b.Cascade_CMS_List_EMail in (
                                select EMail from  Black_List where EMail = b.Cascade_CMS_List_EMail
                                )"""

    MYSQL_CONN = mssql_conn()
    cur = MYSQL_CONN.cursor()
    cur.execute(sql3_, (Cascade_Project_No, Cascade_Send_Data_No))
    select_EmailBlock_exist_data = cur.fetchall()

    Update_EmailBlock = []
    for EmailBlock in select_EmailBlock_exist_data:
        cont_errorEmail = cont_errorEmail + 1
        Update_EmailBlock.append((EmailBlock[2], EmailBlock[0], EmailBlock[6]))

    MYSQL_CONN = mssql_conn()
    cur = MYSQL_CONN.cursor()

    insert_User_Info = """
                                 UPDATE Cascade_EmailScheduleState SET Cascade_Send_Data_Status = '6', SendingStatus = '6' WHERE Cascade_Send_Data_No = %s and Cascade_Project_No = %s and Cascade_EmailScheduleState_No = %s
                                                                                                                                   """
    cur.executemany(insert_User_Info, Update_EmailBlock)

    MYSQL_CONN.commit()
    cur.close()

    fin_item = []
    Update_Cascade_CustomPID_List = []
    Chaeck_Email_Repeat = []
    Chaeck_PID_Repeat = []
    for idx in range(0, len(Cascade_CMS_List_Data)):

        Project_No = Cascade_CMS_List_Data[idx][0]
        Cascade_CMS_List_No = Cascade_CMS_List_Data[idx][1]
        PID = Cascade_CMS_List_Data[idx][3]
        User_email = Cascade_CMS_List_Data[idx][4]
        Name = Cascade_CMS_List_Data[idx][5]
        Cascade_EmailScheduleState_No = Cascade_CMS_List_Data[idx][6]
        CustomPID = Cascade_CMS_List_Data[idx][7]
        Cascade_CustomPID_List_No = Cascade_CMS_List_Data[idx][8]

        if User_email in Chaeck_Email_Repeat:
            pass
        else:
            # 讓重複PID不發送
            if PID in Chaeck_PID_Repeat:
                PID_IsRepeat.append((PID, Cascade_CMS_List_No))
                pass
            else:
                sender = ['GO SURVEY', 'service@dm.gosurvey.com.tw']
                AA = re.split("@", User_email)
                check_email = re.split("@", User_email)
                if User_email == User_email.replace(' ', '') and AA[0] != "":
                    if User_email == User_email.replace(' ', '') and check_email[0] != "" and len(check_email) == 2:
                        if not p.match(User_email):
                            continue
                        else:
                            send_async_email(Cascade_Project_Subject, sender, PID, CustomPID, Name,
                                             Cascade_EmailScheduleState_No, User_email, Cascade_Project_No,
                                             Cascade_Project_Number, Cascade_Gift, Cascade_Project_Start_Time,
                                             Cascade_Project_End_Time, Cascade_Email_data,
                                             Cascade_Project_Email_Subject, Cascade_Send_Data_No,
                                             Cascade_CustomPID_List_No, 'task2')
                            Chaeck_Email_Repeat.append(User_email)
                            Chaeck_PID_Repeat.append(PID)
                            # # 查出這筆Cascade_CustomPID_List有無被寫入的資料
                            # MYSQL_CONN = mssql_conn()
                            # cur = MYSQL_CONN.cursor()
                            #
                            # query_script = """
                            #                             select * from Cascade_CustomPID_List where Cascade_Project_No = %s and Cascade_CustomPID = %s and Cascade_CMS_List_No = %s and Cascade_EmailScheduleState_No = %s
                            #
                            #                                                                    """
                            # cur.execute(query_script, (Project_No, CustomPID, Cascade_CMS_List_No, Cascade_EmailScheduleState_No))
                            # Cascade_CustomPID_List_exist_data = cur.fetchall()
                            # cur.close()
                            #
                            # if Cascade_CustomPID_List_exist_data is not None:
                            #     Update_Cascade_CustomPID_List.append((Cascade_CMS_List_No, Cascade_EmailScheduleState_No, Project_No, CustomPID))
                            # else:
                            #     pass
                    else:
                        MYSQL_CONN = mssql_conn()
                        cur = MYSQL_CONN.cursor()

                        insert_User_Info = """
                                                        UPDATE Cascade_EmailScheduleState SET Cascade_Send_Data_Status = '6', SendingStatus = '6' WHERE Cascade_Send_Data_No = %s and Cascade_Project_No = %s and Cascade_EmailScheduleState_No = %s
                                                                                                                                   """
                        cur.execute(insert_User_Info,
                                    (Cascade_Send_Data_No, Cascade_Project_No, Cascade_EmailScheduleState_No))

                        MYSQL_CONN.commit()
                        cur.close()

                        cont_errorEmail = cont_errorEmail + 1

    await asyncio.sleep(0.1)


async def main():
    global PID_IsRepeat
    MYSQL_CONN = mssql_conn()
    cur = MYSQL_CONN.cursor()
    query_script = """
                  Select TOP 1 a.Cascade_Send_Data_No, a.Cascade_Project_No, a.Cascade_Send_Amount_ALL , a.Cascade_Send_Schedule
                  from Cascade_Send_Data a
                  Left join Cascade_EmailScheduleState b on a.Cascade_Send_Data_No = b.Urge_Send_Data_No
                  where a.Cascade_Send_Status = 0 and b.Urge_Send_Data_No is Null and a.Cascade_Send_Connection = 'KS發信機' ORDER BY a.Cascade_Send_Schedule;
                                                                     """
    cur.execute(query_script)
    myresult_first = cur.fetchall()
    cur.close()
    print(myresult_first)
    now_date_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cont_errorEmail = 0

    which_AWS = "Concat_UAT"

    for x in myresult_first:
        print(x)

        date = datetime.datetime.strptime(str(x[3]), "%Y-%m-%d %H:%M:%S")
        if datetime.datetime.now() > date:
            Cascade_Send_Data_No = int(x[0])
            Cascade_Project_No = int(x[1])

            # 先寫入Cascade_Send_Data發送中狀態
            MYSQL_CONN = mssql_conn()
            cur = MYSQL_CONN.cursor()

            insert_User_Info = """
                                           UPDATE Cascade_Send_Data SET Cascade_Send_Status = '2' WHERE Cascade_Project_No = %s and Cascade_Send_Data_No = %s
                                                                                        """
            cur.execute(insert_User_Info, (Cascade_Project_No, Cascade_Send_Data_No))

            MYSQL_CONN.commit()
            cur.close()

            # 先寫入Cascade_EmailScheduleState發送中狀態
            MYSQL_CONN = mssql_conn()
            cur = MYSQL_CONN.cursor()

            insert_User_Info = """
                                                   UPDATE Cascade_EmailScheduleState SET Cascade_Send_Data_Status = '2' WHERE Cascade_Send_Data_No = %s
                                                                                                """
            cur.execute(insert_User_Info, (Cascade_Send_Data_No))

            MYSQL_CONN.commit()
            cur.close()

            # 建立並行任務
            task1_obj = asyncio.create_task(task1(Cascade_Project_No, Cascade_Send_Data_No))
            task2_obj = asyncio.create_task(task2(Cascade_Project_No, Cascade_Send_Data_No))

            # 等待所有任務完成
            await asyncio.gather(task1_obj, task2_obj)

            # MYSQL_CONN = mssql_conn()
            # cur = MYSQL_CONN.cursor()
            #
            # insert_User_Info = """
            #                                                      UPDATE Cascade_CustomPID_List SET Cascade_CMS_List_No = %s, Cascade_EmailScheduleState_No = %s WHERE Cascade_Project_No = %s and Cascade_CustomPID = %s;
            #                                                                                            """
            #
            # cur.executemany(insert_User_Info,Update_Cascade_CustomPID_List)
            # MYSQL_CONN.commit()
            # cur.close()
            # print("更新Cascade_CustomPID_List筆數:" +str(len(Update_Cascade_CustomPID_List)))
            #

            # 有同專案重複PID就改狀態以後不撈
            if len(PID_IsRepeat) != 0:
                print("進PID_IsRepeat")
                # #UPDATE Cascade_EmailScheduleState 的 SendingStatus
                MYSQL_CONN = mssql_conn()
                cur = MYSQL_CONN.cursor()

                insert_User_Info = """
                                              UPDATE Cascade_CMS_List SET Cascade_CMS_List_Status = '1' WHERE Cascade_CMS_List_PID = %s and Cascade_CMS_List_No = %s
                                                                                                                                    """
                cur.executemany(insert_User_Info, PID_IsRepeat)

                MYSQL_CONN.commit()
                cur.close()

            # UPDATE Cascade_EmailScheduleState 的 SendingStatus
            update_list = []
            limit_update = 1000
            for count_index in range(0, len(Update_Cascade_EmailScheduleState)):
                update_list.append(Update_Cascade_EmailScheduleState[count_index])
                if len(update_list) < limit_update:
                    continue

                Update_Cascade_EmailScheduleState_values = ','.join(map(str, update_list))
                MYSQL_CONN = mssql_conn()
                cur = MYSQL_CONN.cursor()

                insert_User_Info = """
                                                 UPDATE Cascade_EmailScheduleState SET SendingStatus = '1' WHERE Cascade_Send_Data_No = {} and Cascade_Project_No = {} and Cascade_EmailScheduleState_No in ({}) and SendingStatus = 0
                                                                                                                                        """.format(
                    Cascade_Send_Data_No, Cascade_Project_No, Update_Cascade_EmailScheduleState_values)
                cur.execute(insert_User_Info)

                MYSQL_CONN.commit()
                cur.close()

                # UPDATE Cascade_EmailScheduleState  的 Cascade_Send_Data_Status
                MYSQL_CONN = mssql_conn()
                cur = MYSQL_CONN.cursor()

                insert_User_Info = """
                                                UPDATE Cascade_EmailScheduleState SET Cascade_Send_Data_Status = '1' WHERE Cascade_Send_Data_No = {} and Cascade_Project_No = {} and Cascade_EmailScheduleState_No in ({})
                                                                                                                                                """.format(
                    Cascade_Send_Data_No, Cascade_Project_No, Update_Cascade_EmailScheduleState_values)
                cur.execute(insert_User_Info)

                MYSQL_CONN.commit()
                cur.close()

                update_list = []
            if update_list:
                print("update_list")
                print(update_list)
                Update_Cascade_EmailScheduleState_values = ','.join(map(str, update_list))
                MYSQL_CONN = mssql_conn()
                cur = MYSQL_CONN.cursor()

                insert_User_Info = """
                                                                  UPDATE Cascade_EmailScheduleState SET SendingStatus = '1' WHERE Cascade_Send_Data_No = {} and Cascade_Project_No = {} and  Cascade_EmailScheduleState_No in ({}) and SendingStatus = '0'
                                                                                                                                        """.format(
                    Cascade_Send_Data_No, Cascade_Project_No, Update_Cascade_EmailScheduleState_values)
                cur.execute(insert_User_Info)

                MYSQL_CONN.commit()
                cur.close()

                # UPDATE Cascade_EmailScheduleState  的 Cascade_Send_Data_Status
                MYSQL_CONN = mssql_conn()
                cur = MYSQL_CONN.cursor()

                insert_User_Info = """
                                                         UPDATE Cascade_EmailScheduleState SET Cascade_Send_Data_Status = '1' WHERE Cascade_Send_Data_No = {} and Cascade_Project_No = {} and Cascade_EmailScheduleState_No in ({})
                                                                                                                                                """.format(
                    Cascade_Send_Data_No, Cascade_Project_No, Update_Cascade_EmailScheduleState_values)
                cur.execute(insert_User_Info)

                MYSQL_CONN.commit()
                cur.close()

                update_list = []

            print("完成總數：" + str(count))
            # UPDATE Cascade_Send_Data
            # count = count - cont_errorEmail
            Finish_Time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            MYSQL_CONN = mssql_conn()
            cur = MYSQL_CONN.cursor()

            insert_User_Info = """
                                           UPDATE Cascade_Send_Data SET Cascade_Send_Status = '1', Cascade_Send_Amount = %s, Cascade_Send_Finsh_Time = %s WHERE Cascade_Project_No = %s and Cascade_Send_Data_No = %s
                                                                                   """
            cur.execute(insert_User_Info, (count, Finish_Time, Cascade_Project_No, Cascade_Send_Data_No))
            MYSQL_CONN.commit()
            cur.close()

            count = 0
            cont_errorEmail = 0
            Update_Cascade_EmailScheduleState = []
            Update_EmailBlock = []
            Chaeck_Email_Repeat = []
            Chaeck_PID_Repeat = []
            PID_IsRepeat = []
# 啟動事件迴圈
asyncio.run(main())
