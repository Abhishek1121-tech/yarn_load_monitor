import os
import sys
import time
import datetime
from datetime import datetime
from pprint import pprint
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from tabulate import tabulate
import pandas as pd 
import csv



from JobMonitoringUtil import JobMonitoringUtil
from ConfigReader import ConfigReader
from JobMonitoringConstants import JobMonitoringConstants
from MailSender import MailSender

class JobMonitoring:
	
	reader= ConfigReader()
	monitor = JobMonitoringUtil()
	mailsender = MailSender()
	
		
	def __init__(self):
		self.set_prop_get_response()
		
	
		
	def set_prop_get_response(self):
		config_dict=ConfigReader.get_confic_dict();
		currenttimestamp=JobMonitoring.monitor.getCurrentTimeStampInMs()
		getlasttimeepoch=JobMonitoring.reader.get_config_section_lasttime_epoch()
		lasttimeepoch=getlasttimeepoch[JobMonitoringConstants.LASTTIMEEPOCH][JobMonitoringConstants.LASTTIMESTAMP]
		response_url=JobMonitoring.monitor.getURLMonitoringJob(currenttimestamp,lasttimeepoch)
		#print(response_url)
		response=JobMonitoringConstants.BALNK_STRING
		try:
			response=JobMonitoring.get_response(response_url)
		except Exception as e:
				message_mail=self.send_mail_On_Failure(currenttimestamp,lasttimeepoch,config_dict,JobMonitoringConstants.BALNK_STRING,str(e))
				print(message_mail)
		if  response != JobMonitoringConstants.BALNK_STRING:
			if response.status_code == JobMonitoringConstants.STATUS_200:
				 list_res_get=JobMonitoring.parse_response(response,currenttimestamp)
				 if list_res_get == JobMonitoringConstants.FINE_MSG:
					 print(str(datetime.fromtimestamp(int(currenttimestamp)/int(JobMonitoringConstants.VALUE_1000)).strftime(JobMonitoringConstants.SIMPLE_DATA_TIME_FORMAT))+JobMonitoringConstants.SPACE+JobMonitoringConstants.OK_MSG)
					 JobMonitoring.reader.set_config(JobMonitoringConstants.LASTTIMEEPOCH,JobMonitoringConstants.LASTTIMESTAMP,currenttimestamp)
				 else:
					 message_mail=self.send_mail_On_Success(currenttimestamp,lasttimeepoch,config_dict,list_res_get)
					 if str(message_mail) == JobMonitoringConstants.NONE:
						 JobMonitoring.reader.set_config(JobMonitoringConstants.LASTTIMEEPOCH,JobMonitoringConstants.LASTTIMESTAMP,currenttimestamp)
					 else:
						 print(JobMonitoringConstants.MAIL_CONF_ERR_MSG)
			else:
				message_mail=self.send_mail_On_Failure(currenttimestamp,lasttimeepoch,config_dict,str(response.status_code),str(response.text))
				print(message_mail)
		else:
			print(JobMonitoringConstants.URL_CONF_ERR_MSG)
		
	def send_mail_On_Failure(self,currenttimestamp,lasttimeepoch,config_dict,status_code,text):
		message=self.set_message_prop(config_dict)
		plainhtml= self.getPlainHtmlRequestFailed(config_dict,currenttimestamp,lasttimeepoch,status_code,text)
		message_part = MIMEText(plainhtml, JobMonitoringConstants.HTML)
		message.attach(message_part)
		mail_response_message=JobMonitoring.mailsender.send_mail_verify(config_dict[JobMonitoringConstants.MAIL_EXTRA_PROP][JobMonitoringConstants.JOB_FAILED_RECIVERS],message)
		return mail_response_message
		
	def get_response(response_url):
		return JobMonitoring.monitor.getURLResponse(response_url)
		
	def parse_response(response_text,currenttimestamp):
		data= False
		if response_text.text:
			try:
				responses = response_text.json()
			except ValueError:
				print("Json is empty")
		if responses:
			try:
				data = responses[JobMonitoringConstants.JSONKEYAPPS][JobMonitoringConstants.JSONKEYAPP]
			except (IndexError, KeyError, TypeError):
				print(str(datetime.fromtimestamp(int(currenttimestamp)/int(JobMonitoringConstants.VALUE_1000)).strftime(JobMonitoringConstants.SIMPLE_DATA_TIME_FORMAT))+JobMonitoringConstants.SPACE+JobMonitoringConstants.JSON_KEY_ERR_MSG)
		if data:
			return JobMonitoring.get_App_Id_Name(data,len(data))
		else:
			return (JobMonitoringConstants.FINE_MSG)
		
		
	def get_App_Id_Name(data,length_data):
		app_string=[]
		for indx,app in enumerate(data):
			app_indx_string=[]
			app_indx_string.append(data[indx][JobMonitoringConstants.APP_ID_KEY])
			app_indx_string.append(data[indx][JobMonitoringConstants.APP_NAME_KEY])
			app_indx_string.append(datetime.fromtimestamp(int(data[indx][JobMonitoringConstants.JOBFINISHEDTIMEKEY])/int(JobMonitoringConstants.VALUE_1000)).strftime(JobMonitoringConstants.SIMPLE_DATA_TIME_FORMAT))
			app_string.append(app_indx_string)
		return app_string
		
	def send_mail_On_Success(self,currenttimestamp,lasttimeepoch,config_dict,list_res_get):
		message=self.set_message_prop(config_dict)
		dataframe_response = pd.DataFrame(list_res_get,columns = [(JobMonitoringConstants.APPLICATION+JobMonitoringConstants.APP_ID_KEY).upper(),(JobMonitoringConstants.APPLICATION+JobMonitoringConstants.APP_NAME_KEY).upper(),(JobMonitoringConstants.JOBFINISHEDTIMEKEY).upper()])
		
		dataframe_response.to_csv(config_dict[JobMonitoringConstants.MAIL_EXTRA_PROP][JobMonitoringConstants.JOB_LIST_CSV_NAME],index=False)
		with open(config_dict[JobMonitoringConstants.MAIL_EXTRA_PROP][JobMonitoringConstants.JOB_LIST_CSV_NAME]) as input_file:
			__csvReader = csv.reader(input_file)
			list_data = list(__csvReader)
		
		plainhtml= self.getPlainHtmlJobFailed(config_dict,currenttimestamp,lasttimeepoch)
		plainhtml = plainhtml.format(table=tabulate(list_data, headers=JobMonitoringConstants.FIRSTROW, tablefmt=JobMonitoringConstants.HTML))

		message_part = MIMEText(plainhtml, JobMonitoringConstants.HTML)
		message.attach(message_part)
		mail_response_message=JobMonitoring.mailsender.send_mail_verify(config_dict[JobMonitoringConstants.MAIL_EXTRA_PROP][JobMonitoringConstants.JOB_FAILED_RECIVERS],message)
		return mail_response_message


	def set_message_prop(self,config_dict):
		message = MIMEMultipart(JobMonitoringConstants.ALTERNATIVE)
		message[JobMonitoringConstants.SUBJECT] = config_dict[JobMonitoringConstants.MAIL_EXTRA_PROP][JobMonitoringConstants.CLIENT]+JobMonitoringConstants.SPACE+config_dict[JobMonitoringConstants.MAIL_EXTRA_PROP][JobMonitoringConstants.ENV]+JobMonitoringConstants.SPACE+JobMonitoringConstants.JOB_FAILED_SUBJECT
		message[JobMonitoringConstants.FROM]= config_dict[JobMonitoringConstants.SMTP_DICT][JobMonitoringConstants.SMTP_SENDER]
		message[JobMonitoringConstants.TO] = config_dict[JobMonitoringConstants.MAIL_EXTRA_PROP][JobMonitoringConstants.JOB_FAILED_RECIVERS]
		return message
		
		
	def getPlainHtmlJobFailed(self,config_dict,currenttimestamp,lasttimeepoch):
		return """
		<html><head>
		<style> 
		table, th, td {{  border-collapse: collapse; }}
		th, td {{ padding: 5px;  border: 1px solid black;}}
		</style>
		</head>
		<body>
		<p>Dear Admin,<br>
		There are some critical issue, Have to look into this.</p>
		Failed Jobs<br>
		<b>Jobs are Failed on %s %s between Time Range %s and %s JST<br></b><br>
		Below are the listed Application:<br></p><br>
		{table}
		<p>Regards,<br>
		Monitoring Team.<br>
		This is an automated e-mail alert to help you keep track of server. Hence, please do not reply to this e-mail.</p>
		</body></html>"""%(config_dict[JobMonitoringConstants.MAIL_EXTRA_PROP][JobMonitoringConstants.CLIENT],config_dict[JobMonitoringConstants.MAIL_EXTRA_PROP][JobMonitoringConstants.ENV], datetime.fromtimestamp(int(lasttimeepoch)/int(JobMonitoringConstants.VALUE_1000)).strftime(JobMonitoringConstants.SIMPLE_DATA_TIME_FORMAT), datetime.fromtimestamp(int(currenttimestamp)/int(JobMonitoringConstants.VALUE_1000)).strftime(JobMonitoringConstants.SIMPLE_DATA_TIME_FORMAT))

	def getPlainHtmlRequestFailed(self,config_dict,currenttimestamp,lasttimeepoch,status_code,text):
		return """
		<html>
		<body>
		<p>Dear Admin,<br>
		URL for the Job Failed status not working. Please look into this.</p>
		Monitoring Failed<br>
		<b>Monitoring Failed on %s %s between Time Range %s and %s<br></b><br>
		Response for the URL:<br></p><br>
		Response Code : %s<br>
		Response Text : %s<br>
		<p>Regards,<br>
		Monitoring Team.<br>
		This is an automated e-mail alert to help you keep track of server. Hence, please do not reply to this e-mail.</p>
		</body></html>"""%(config_dict[JobMonitoringConstants.MAIL_EXTRA_PROP][JobMonitoringConstants.CLIENT],config_dict[JobMonitoringConstants.MAIL_EXTRA_PROP][JobMonitoringConstants.ENV], datetime.fromtimestamp(int(lasttimeepoch)/int(JobMonitoringConstants.VALUE_1000)).strftime(JobMonitoringConstants.SIMPLE_DATA_TIME_FORMAT), datetime.fromtimestamp(int(currenttimestamp)/int(JobMonitoringConstants.VALUE_1000)).strftime(JobMonitoringConstants.SIMPLE_DATA_TIME_FORMAT),status_code,text)
		
	def getPlainTextHeaderJobFailed(self,config_dict):
		return """\
		Dear Admin,
		There are some critical issue, Have to look into this."""
	
	def getPlainTextFooter(self,config_dict):
		return """\
		Regards,
		Monitoring Team.
		This is an automated e-mail alert to help you keep track of server. Hence, please do not reply to this e-mail. """
		
monitoring = JobMonitoring()	




