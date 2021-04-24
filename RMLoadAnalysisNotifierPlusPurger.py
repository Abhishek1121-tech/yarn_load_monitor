
import os
import sys
import time
import datetime
from datetime import datetime
from pprint import pprint
import json
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from email.mime.multipart import MIMEMultipart
from tabulate import tabulate
import pandas as pd 
import csv
from datetime import datetime, timedelta

from JobMonitoringUtil import JobMonitoringUtil
from ConfigReader import ConfigReader
from JobMonitoringConstants import JobMonitoringConstants
from MailSender import MailSender


class RMLoadAnalysisNotifierPlusPurger:

    reader= ConfigReader()
    monitor = JobMonitoringUtil()
    mailsender = MailSender()

    def __init__(self):
        self.runAggregate()

    def runAggregate(self):
        config_dict=ConfigReader.get_confic_dict()
        LAST_PROCESSED_FILE_NAME=config_dict[JobMonitoringConstants.LOADANALYSIS][JobMonitoringConstants.last_processed_file_name]
        if LAST_PROCESSED_FILE_NAME  != JobMonitoringConstants.BALNK_STRING:
            print(LAST_PROCESSED_FILE_NAME)
            message_mail=self.aggregateAllocatedMemory(config_dict,LAST_PROCESSED_FILE_NAME)
            if str(message_mail) == JobMonitoringConstants.NONE:
                    status=RMLoadAnalysisNotifierPlusPurger.monitor.deleteFileFromLocalFileSystem(config_dict[JobMonitoringConstants.LOADANALYSIS][JobMonitoringConstants.concanate_data_in_file_path_prefix]+LAST_PROCESSED_FILE_NAME)
                    if (status == True):
                        print('File deleted Successfully'+LAST_PROCESSED_FILE_NAME)
                        self.setFileNameForNextDay(config_dict)
                    else:
                        print('Exception check logs, File is filling Up ... !!')
            else:
                print(JobMonitoringConstants.MAIL_CONF_ERR_MSG)
        else:
            print('empty, run for the first time')
            currentDate=datetime.now().strftime(JobMonitoringConstants.SIMPLE_DATE_FORMAT)
            filename_prefix=config_dict[JobMonitoringConstants.LOADANALYSIS][JobMonitoringConstants.concanate_data_in_file_name]
            filename_extension=config_dict[JobMonitoringConstants.LOADANALYSIS][JobMonitoringConstants.concanate_data_in_file_ext]
            filename_path_prefix=config_dict[JobMonitoringConstants.LOADANALYSIS][JobMonitoringConstants.concanate_data_in_file_path_prefix]
            last_processed_file_name=filename_prefix+JobMonitoringConstants.DOLLAR+str(currentDate)+JobMonitoringConstants.DOT+filename_extension
            print(last_processed_file_name)
            RMLoadAnalysisNotifierPlusPurger.reader.set_config_config_properties(JobMonitoringConstants.LOADANALYSIS,JobMonitoringConstants.last_processed_file_name,last_processed_file_name)
            print('Update the value according to current date'+last_processed_file_name)

    def aggregateAllocatedMemory(self,config_dict,last_processed_file_name):
        try:
            filename_path_prefix=config_dict[JobMonitoringConstants.LOADANALYSIS][JobMonitoringConstants.concanate_data_in_file_path_prefix]
            file_with_pathname=filename_path_prefix+last_processed_file_name
            df = pd.read_csv(file_with_pathname)
            agg_value=self.get_agg_value(df,config_dict)
            print(agg_value)
            self.aggregate_avg_d_day_mailSend(agg_value,file_with_pathname,last_processed_file_name,config_dict)
        except Exception as e:
            print(e)
            raise e 

    def get_agg_value(self,df,config_dict):
        if config_dict[JobMonitoringConstants.LOADANALYSIS][JobMonitoringConstants.aggregate_in_unit] == JobMonitoringConstants.TB:
            return str(round(((df[JobMonitoringConstants.allocatedMB].sum()/df[JobMonitoringConstants.allocatedMB].count())/JobMonitoringConstants.NUM_1024)/JobMonitoringConstants.NUM_1024,JobMonitoringConstants.NUM_2))+JobMonitoringConstants.SPACE+JobMonitoringConstants.TB
        elif config_dict[JobMonitoringConstants.LOADANALYSIS][JobMonitoringConstants.aggregate_in_unit] == JobMonitoringConstants.GB:
            return str(round((df[JobMonitoringConstants.allocatedMB].sum()/df[JobMonitoringConstants.allocatedMB].count())/JobMonitoringConstants.NUM_1024,JobMonitoringConstants.NUM_2))+JobMonitoringConstants.SPACE+JobMonitoringConstants.GB
        else:
            raise ValueError(JobMonitoringConstants.UNIT_ERR_MSG)

    def aggregate_avg_d_day_mailSend(self,agg_value,file_with_pathname,last_processed_file_name,config_dict):
        print('file_with_pathname'+' '+file_with_pathname)
        print(file_with_pathname.split(JobMonitoringConstants.DOLLAR)[JobMonitoringConstants.NUM_1].split(JobMonitoringConstants.DOT)[JobMonitoringConstants.NUM_0])
        file_date=file_with_pathname.split(JobMonitoringConstants.DOLLAR)[JobMonitoringConstants.NUM_1].split(JobMonitoringConstants.DOT)[JobMonitoringConstants.NUM_0]
        message=self.set_message_prop(config_dict)
        plainTextHtml=self.plainTextAvgAgg(agg_value,file_date,config_dict)
        message_part = MIMEText(plainTextHtml, JobMonitoringConstants.HTML)
        message.attach(message_part)
        try:
            with open(file_with_pathname, "rb") as attachment:
                part=MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header("Content-Disposition","attachment", filename=last_processed_file_name)
                message.attach(part)
        except Exception as e:
            print(e)
            raise e
        mail_response_message=RMLoadAnalysisNotifierPlusPurger.mailsender.send_mail_verify(config_dict[JobMonitoringConstants.LOADANALYSIS][JobMonitoringConstants.AVG_AGG_RECIVERS],message)
        return mail_response_message

    def set_message_prop(self,config_dict):
        message = MIMEMultipart(JobMonitoringConstants.ALTERNATIVE)
        message[JobMonitoringConstants.SUBJECT] = config_dict[JobMonitoringConstants.MAIL_EXTRA_PROP][JobMonitoringConstants.CLIENT]+JobMonitoringConstants.SPACE+config_dict[JobMonitoringConstants.MAIL_EXTRA_PROP][JobMonitoringConstants.ENV]+JobMonitoringConstants.SPACE+JobMonitoringConstants.AGG_AVG_INFO
        message[JobMonitoringConstants.FROM]= config_dict[JobMonitoringConstants.SMTP_DICT][JobMonitoringConstants.SMTP_SENDER]
        message[JobMonitoringConstants.TO] = config_dict[JobMonitoringConstants.LOADANALYSIS][JobMonitoringConstants.AVG_AGG_RECIVERS]
        return message

    def setFileNameForNextDay(self,config_dict):
        currentDate=datetime.now().strftime(JobMonitoringConstants.SIMPLE_DATE_FORMAT)
        filename_prefix=config_dict[JobMonitoringConstants.LOADANALYSIS][JobMonitoringConstants.concanate_data_in_file_name]
        filename_extension=config_dict[JobMonitoringConstants.LOADANALYSIS][JobMonitoringConstants.concanate_data_in_file_ext]
        filename_path_prefix=config_dict[JobMonitoringConstants.LOADANALYSIS][JobMonitoringConstants.concanate_data_in_file_path_prefix]
        processed_file_name=filename_prefix+JobMonitoringConstants.DOLLAR+str(currentDate)+JobMonitoringConstants.DOT+filename_extension
        print('processed_file_name'+processed_file_name)
        RMLoadAnalysisNotifierPlusPurger.reader.set_config_config_properties(JobMonitoringConstants.LOADANALYSIS,JobMonitoringConstants.last_processed_file_name,processed_file_name)

    def plainTextAvgAgg(self,agg_value,date,config_dict):
                return """
                <html>
                <body>
                <p>Dear Admin,<br>
                Average Aggregated Cluster Yarn for the day <b>%s</b> is <b>%s</b></p>
                Please find the attachmnet for the day for more analysis.
                <p>Regards,<br>
                Monitoring Team.<br>
                This is an automated e-mail alert to help you keep track of server. Hence, please do not reply to this e-mail.</p>
                </body></html>"""%(date,agg_value)


rmLoadAnalysisNotifierPlusPurger = RMLoadAnalysisNotifierPlusPurger()
