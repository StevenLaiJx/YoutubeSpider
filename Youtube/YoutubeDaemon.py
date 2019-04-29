# -*- coding: utf-8 -*-

import sys, os, platform
import scrapy
from scrapy.utils.project import get_project_settings
from scrapy import cmdline
import json
import pymysql.cursors
import time, datetime
import uuid
import random
import logging
import psutil
import subprocess

# Enable below codes if running in python 2.7.X
#reload(sys) 
#sys.setdefaultencoding('utf8')

class YoutubeSpiderDaemon(object):
    search_keywords = []
    
    # Initialization
    def __init__(self): 
        # Initialize Logger
        log_file = 'YoutubeSpiderDaemon_' + datetime.datetime.now().strftime('%Y%m%d') + '.LOG'
        log_path = os.path.join(os.getcwd(), log_file)
        log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        self.log_handler = logging.FileHandler(log_path)
        self.log_handler.setLevel(logging.INFO)
        self.log_handler.setFormatter(log_formatter)        
        self.logger = logging.getLogger('YoutubeSpiderDaemon')
        self.logger.setLevel(level = logging.INFO)
        self.logger.addHandler(self.log_handler)
        
        # Project settings
        self.settings = get_project_settings()
        if os.access(self.settings.get('MY_CONFIG_PATH'), os.F_OK):
            with open(self.settings.get('MY_CONFIG_PATH'), 'r') as fb:
                self.config_parameters = json.loads(fb.read())
        else:
            self.logger.info('Please set valid config file(%s)' % self.settings.get('MY_CONFIG_PATH'))
            sys.exit()
             
        # Open database
        self.db_connector = None
        if 'mysql' == self.config_parameters['database']['type']:
            self.db_connector = pymysql.connect(
                host = str(self.config_parameters['database']['host']),
                port = int(self.config_parameters['database']['port']),
                user = str(self.config_parameters['database']['user']),
                passwd = str(self.config_parameters['database']['password']),
                db = str(self.config_parameters['database']['dbname']),
                charset = str(self.config_parameters['database']['charset']),
                cursorclass = pymysql.cursors.DictCursor)
                
        # Get keywords without downloading
        with self.db_connector.cursor() as cursor:
            sql = "SELECT Keywords FROM tKeywords WHERE Source='Youtube' AND Result!=1;";
            cursor.execute(sql)
            result_rows = cursor.fetchall()
            for result_row in result_rows:
                keywords = result_row['Keywords']
                if keywords is not None and len(keywords) > 0:
                    self.search_keywords.append(keywords)
        
        # Initialize parent class
        super(YoutubeSpiderDaemon, self).__init__()
            
    # Run spider
    def run(self):
        # Start spider keywords by keywords
        for keywords in self.search_keywords:
            cmd_line = 'scrapy crawl YoutubeSpider -a keywords=%s' % keywords
            self.logger.info('cmd_line=%s' % cmd_line)
            cmdline.execute(cmd_line.split())
            time.sleep(3) 
        
        # Close database connection
        self.db_connector.close()

if __name__ == '__main__':
    YoutubeSpiderDaemon().run()