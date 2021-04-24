import os
import sys
import time
import datetime
from datetime import datetime
from pprint import pprint
import json
import csv

from JobMonitoringUtil import JobMonitoringUtil
from ConfigReader import ConfigReader
from JobMonitoringConstants import JobMonitoringConstants
from MailSender import MailSender

class RMLoadAnalysisJob:

    reader= ConfigReader()
    monitor = JobMonitoringUtil()
    mailsender = MailSender()

    def __init__(self):
        self.set_prop_write_info()

    def set_prop_write_info(self):
        config_dict=ConfigReader.get_confic_dict()
        currentDate=RMLoadAnalysisJob.monitor.getCurrentDate()
        currenttimestamp=RMLoadAnalysisJob.monitor.getCurrentTimeStampInMs()
        filename_prefix=config_dict[JobMonitoringConstants.LOADANALYSIS][JobMonitoringConstants.concanate_data_in_file_name]
        filename_extension=config_dict[JobMonitoringConstants.LOADANALYSIS][JobMonitoringConstants.concanate_data_in_file_ext]
        filename_path_prefix=config_dict[JobMonitoringConstants.LOADANALYSIS][JobMonitoringConstants.concanate_data_in_file_path_prefix]
        schema=JobMonitoringConstants.LOADANALYSIS_SCHEMA
        filename=filename_path_prefix+filename_prefix+JobMonitoringConstants.DOLLAR+str(currentDate)+JobMonitoringConstants.DOT+filename_extension
        RMLoadAnalysisJob.monitor.createFileIfNotExistsWithSchema(filename,schema)
        request_url=RMLoadAnalysisJob.monitor.getStatisticsURL()
        print(request_url)
        response=JobMonitoringConstants.BALNK_STRING
        try:
            response=RMLoadAnalysisJob.monitor.getURLResponse(request_url)
        except Exception as e:
				#message_mail=self.send_mail_On_Failure(currenttimestamp,lasttimeepoch,config_dict,JobMonitoringConstants.BALNK_STRING,str(e))
                print(e)
                raise e
        if  response != JobMonitoringConstants.BALNK_STRING:
            if response.status_code == JobMonitoringConstants.STATUS_200:
                list_res_get=self.parse_response(response,currenttimestamp,filename)

    def parse_response(self,response,currenttimestamp,filename):
        data=False
        if response.text:
            try:
                responses = response.json()
            except ValueError:
                print("Json is empty")
            if responses:
                try:
                    data = responses[JobMonitoringConstants.LOADANALYSIS_CLUSTER_METRICS]
                except (IndexError, KeyError, TypeError) as e:
                    #print(str(datetime.fromtimestamp(int(currenttimestamp)/int(JobMonitoringConstants.VALUE_1000)).strftime(JobMonitoringConstants.SIMPLE_DATA_TIME_FORMAT))+JobMonitoringConstants.SPACE+JobMonitoringConstants.JSON_KEY_ERR_MSG)
                    print(e)
                    raise e
            if data:
                return self.write_metrics_in_file_persist(data,currenttimestamp,filename)
            else:
                print("Cluster metric json is empty")
                return JobMonitoringConstants.URL_CONF_ERR_MSG

    def write_metrics_in_file_persist(self,data,currenttimestamp,filename):
        line=[]
        line.append(currenttimestamp)
        line.append(data['totalMB'])
        line.append(data['availableMB'])
        line.append(data['allocatedMB'])
        print(line)
        self.persistInExistingFile(line,filename)

    def persistInExistingFile(self,line,filename):
        try:
            with open(filename, 'a', newline=JobMonitoringConstants.SINGLE_QUOTE_BLANK_STRING) as f:
                writer = csv.writer(f)
                writer.writerow(line)
                f.close()
        except (IOError,EOFError) as e:
            print("Exception while appending data in existing file for load average. {}".format(e.args[-1]))
            raise e
            
        
        
rmLoadAnalysisJob = RMLoadAnalysisJob() 