import configparser
from configobj import ConfigObj


class ConfigReader:
	def get_config_section(self):
		config = configparser.RawConfigParser()
		config.read('./config.properties')
		if not hasattr(config, 'section_dict'):
			config.section_dict = dict()
			for section in config.sections():
				config.section_dict[section] = dict(config.items(section))
			return config.section_dict
		
	def set_config(self,section,key,value):
		config = configparser.ConfigParser()
		config[section] = {key: value}
		
		config_dict = self.get_config_section()
		with open(config_dict['EPOCH']['lasttimeconfigfile'], 'w') as configfile:
			config.write(configfile)
		return "true"
	
	def set_config_config_properties(self,section,key,value):
		try:
			config = ConfigObj('./config.properties')
			print('Updating value for section '+section+' key '+key)
			config[section][key]=value
			config.write()
		except Exception as e:
			print(e)
			raise e
	
	def get_config_section_lasttime_epoch(self):
		config = configparser.RawConfigParser()
		config_dict = self.get_config_section()
		config.read(config_dict['EPOCH']['lasttimeconfigfile'])
		if not hasattr(config, 'section_dict'):
			config.section_dict = dict()
			for section in config.sections():
				config.section_dict[section] = dict(config.items(section))
			return config.section_dict
	 
	def get_confic_dict():
		reader=ConfigReader()
		return reader.get_config_section()
