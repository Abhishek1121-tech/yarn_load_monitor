import time
import requests
import json
from pprint import pprint
from JobMonitoringConstants import JobMonitoringConstants
from ConfigReader import ConfigReader	
import os.path
from datetime import date
from datetime import datetime, timedelta

class JobMonitoringUtil:
	def getCurrentTimeStampInMs(self):
		timeStringinms = int(round(time.time() * 1000))
		return timeStringinms
		
	def getURLResponse(self,url):
		return requests.get(url)
		
	def getURLMonitoringJob(self,cuurenttimestamp,previoustimestamp):
		reader= ConfigReader()
		config_dict = reader.get_config_section()
		constants=JobMonitoringConstants()
		return config_dict['APP']['http_scheme'] + constants.COLON + constants.DOUBLEFORWARDSLASH + config_dict['APP']['job_monitor_ip'] + constants.COLON + config_dict['APP']['job_monitor_port'] + config_dict['APP']['url_prefix'] + config_dict['APP']['url_prefix'] + constants.JOBSTATESKEY +constants.EQUALS + config_dict['APP']['states'] + constants.AMPERCENT + constants.JOBSTATESFINALSTATUSKEY + constants.EQUALS + config_dict['APP']['final_status'] + constants.AMPERCENT + constants.JOBSTATESTYPE + constants.EQUALS + config_dict['APP']['application_types'] + constants.AMPERCENT + constants.JOBFINISHEDTIMEBEGINKEY + constants.EQUALS + str(previoustimestamp) + constants.AMPERCENT + constants.JOBFINISHEDTIMEENDKEY + constants.EQUALS + str(cuurenttimestamp)

	def getStatisticsURL(self):
		reader= ConfigReader()
		config_dict = reader.get_config_section()
		constants=JobMonitoringConstants()
		return config_dict['APP']['http_scheme'] + constants.COLON + constants.DOUBLEFORWARDSLASH + config_dict['APP']['job_monitor_ip'] + constants.COLON + config_dict['APP']['job_monitor_port'] + config_dict[constants.LOADANALYSIS][constants.loadanalysis_url_prefix] 

	def convert_to_list(self,string,separtor):
		li = list(string.split(separtor)) 
		return li 

	def createFileIfNotExistsWithSchema(self,filename,schema):
		file_exists = os.path.isfile(filename)
		if not file_exists:
			try:
				file=open(filename, "w")
				file.write(schema+JobMonitoringConstants.NEW_LINE)
				file.close()
			except (IOError,EOFError) as e:
				print("Exception while creating file for load average. {}".format(e.args[-1]))
				raise e

	def getCurrentDate(self):
		today = date.today()
		constants=JobMonitoringConstants()
		return today.strftime(constants.SIMPLE_DATE_FORMAT)

	def getDateMinusDays(self,currentdate,minusdays):
		return currentdate + timedelta(days=-minusdays)
	
	def getDatePlusDays(self,currentdate,plusdays):
		return currentdate + timedelta(days=+plusdays)

	def deleteFileFromLocalFileSystem(self,file):
		try:
			os.remove(file)
			return True
		except OSError as e:  
    			print ("Error: %s - %s." % (e.filename, e.strerror))
		return False

		
