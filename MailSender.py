import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import base64
from ConfigReader import ConfigReader
from JobMonitoringConstants import JobMonitoringConstants


class MailSender:
	
	HOST_NAME=""
	HOST_PORT=""
	ALLOWSTARTTLS=""
	ALLOWSSL=""
	SENDER=""
	PASSWORD=""
	
	def __init__(self):
		config_dict=ConfigReader.get_confic_dict();
		self.HOST_NAME=config_dict[JobMonitoringConstants.SMTP_DICT][JobMonitoringConstants.HOST_NAME]
		self.HOST_PORT=config_dict[JobMonitoringConstants.SMTP_DICT][JobMonitoringConstants.HOST_PORT]
		self.ALLOWSSL=config_dict[JobMonitoringConstants.SMTP_DICT][JobMonitoringConstants.SMTP_ALLOWSSL]
		self.ALLOWSTARTTLS=config_dict[JobMonitoringConstants.SMTP_DICT][JobMonitoringConstants.SMTP_ALLOWSTARTTLS]
		self.SENDER=config_dict[JobMonitoringConstants.SMTP_DICT][JobMonitoringConstants.SMTP_SENDER]
		self.PASSWORD=str(base64.b64decode(config_dict[JobMonitoringConstants.SMTP_DICT][JobMonitoringConstants.SMTP_PASSWORD]),"utf-8")
		
		
		
	def send_mail_verify(self,reciver,message):
		if self.ALLOWSSL == JobMonitoringConstants.TRUE:
			try:
				self.send_mail_ssl(reciver,message)
			except Exception as e:
				return e
		elif self.ALLOWSTARTTLS == JobMonitoringConstants.TRUE:
			try:
				self.send_mail_tls(reciver,message)
			except Exception as e:
				return e
		elif self.ALLOWSTARTTLS == JobMonitoringConstants.FALSE:
			try:
				self.send_mail_authno(reciver,message)
			except Exception as e:
				print(e)
				return e
		else:
			return "if more please add"
		
		
	def send_mail_ssl(self,reciver,message):
		context = ssl.create_default_context()
		with smtplib.SMTP_SSL(self.HOST_NAME, self.HOST_PORT, context=context) as server:
			server.login(self.SENDER,self.PASSWORD)
			server.sendmail(self.SENDER, reciver.split(JobMonitoringConstants.COMMA), message.as_string())
			server.quit()

		
		
	def send_mail_tls(self,reciver,message):
		context = ssl.create_default_context()
		with smtplib.SMTP(self.HOST_NAME, self.HOST_PORT) as server:
			server.ehlo()  # Can be omitted
			server.starttls(context=context)
			server.ehlo()  # Can be omitted
			server.login(self.SENDER,self.PASSWORD)
			server.sendmail(self.SENDER, reciver.split(JobMonitoringConstants.COMMA), message.as_string())
			server.quit()
		
	def send_mail_authno(self,reciver,message):
		with smtplib.SMTP(self.HOST_NAME, self.HOST_PORT) as server:
			server.sendmail(self.SENDER, reciver.split(JobMonitoringConstants.COMMA), message.as_string())
			server.quit()

