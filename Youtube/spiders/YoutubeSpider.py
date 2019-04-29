# -*- coding: utf-8 -*-
import sys, os, platform
import scrapy
from scrapy import signals
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from Youtube.items import YoutubeItem
from scrapy.utils.project import get_project_settings
import base64, json
import pymysql.cursors
import time, datetime
import uuid
import random
import logging

# Enable below codes if running in python 2.7.X
#reload(sys) 
#sys.setdefaultencoding('utf8')

log_file = 'YoutubeSpider_' + datetime.datetime.now().strftime('%Y%m%d') + '.LOG'
log_path = os.path.join(os.getcwd(), log_file)
logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s %(message)s')

# Start Command: scrapy crawl YoutubeSpider -a keywords=
class YoutubespiderSpider(scrapy.Spider):
    name = 'YoutubeSpider'
    search_keywords = ''
    filter_rule = ''
    max_file_size = 10 * 1024 * 1024
    max_file_number = 10
    request_headers = {}
    request_user_agents = []
    request_proxies = []
    total_video_number = 0
    total_download_time = int(time.time())
    month_abbrs = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    min_interval = 0
    max_interval = 0
    
    # Initialization
    def __init__(self, keywords):
        # Handle input
        if keywords is None or len(keywords) <= 0:
            self.logger.info('Please input valid keywords and order rule')
            sys.exit()
        self.search_keywords = keywords
        
        # Project settings
        self.settings = get_project_settings()
        if os.access(self.settings.get('MY_CONFIG_PATH'), os.F_OK):
            with open(self.settings.get('MY_CONFIG_PATH'), 'r') as fb:
                self.config_parameters = json.loads(fb.read())
        else:
            self.logger.info('Please set valid config file(%s)' % self.settings.get('MY_CONFIG_PATH'))
            sys.exit()
        
        # Request headers        
        self.request_headers = self.config_parameters.get('requestHeaders')
        if self.request_headers is None:
            self.request_headers = {
                'Host': 'www.youtube.com',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'zh-CN,zh;q=0.9',
                'Origin': 'https://www.youtube.com'}
        
        # Request user agents
        self.request_user_agents = self.config_parameters.get('requestUserAgents')
        if self.request_user_agents is None or len(self.request_user_agents) <= 0:
            self.request_user_agents = ['Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36']
        
        # Filters
        filters = self.config_parameters.get('filters')
        if filters is not None:
            filter_by = filters.get('filterBy')
            allowed_filters = filters.get('allowedFilters')
            if filter_by is not None and allowed_filters is not None and len(allowed_filters) > 0:
                if filter_by.isdigit() and int(filter_by) > 0 and int(filter_by) <= len(allowed_filters):
                    metadata = allowed_filters[int(filter_by) - 1].get('metadata')
                    if metadata is not None:
                        self.filter_rule = ''
                        for key, value in metadata.items():
                            if '' != self.filter_rule:
                                self.filter_rule = self.filter_rule + '&'
                            self.filter_rule = self.filter_rule + key + '=' + value
                else:
                    filter_by = filter_by.lower()
                    for filter in allowed_filters:
                        filter_type = filter.get('type')
                        if filter_type is not None and filter_by == filter_type.lower():
                            metadata = filter.get('metadata')
                            if metadata is not None:
                                self.filter_rule = ''
                                for key, value in metadata.items():
                                    if '' != self.filter_rule:
                                        self.filter_rule = self.filter_rule + '&'
                                    self.filter_rule = self.filter_rule + key + '=' + value
                            break
                            
        # File configuration
        file = self.config_parameters.get('file')
        if file is not None:
            self.max_file_size = file.get('allowedSize', 10 * 1024 * 1024)
            self.max_file_number = file.get('allowedNumber', 10)
            
        # Proxies
        self.request_proxies = self.config_parameters.get('proxies')
        if self.request_proxies is None or len(self.request_proxies) <= 0:
            self.request_proxies = ['http://127.0.0.1:1080']
            
        # Intervals        
        self.min_interval = float(self.config_parameters['intervals']['requestInterval'])
        self.max_interval = float(self.config_parameters['intervals']['requestInterval']) + 2
             
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
        
        # Initialize parent class
        super(YoutubespiderSpider, self).__init__()
        
    # Closed
    def closed(self, reason):
        # Update download result
        with self.db_connector.cursor() as cursor:
            sql = ('UPDATE tKeywords SET Result=1,LastUpdatedTime=CURRENT_TIMESTAMP() '
                'WHERE Source=%s AND Keywords=%s;')
            cursor.execute(
                sql, 
                (
                str('Youtube'),
                str(self.search_keywords)
                )
                )
            self.db_connector.commit() 
        
        # Close database connection
        self.db_connector.close()
        
        self.logger.info(
            'Youtube spider closed, it took %s seconds to download %s videos' % 
            (int(time.time()) - self.total_download_time, self.total_video_number))
        
    # Start search request
    def start_requests(self):    
        request_url = 'https://www.youtube.com/results?search_query=' + self.search_keywords
        if self.filter_rule is not None and len(self.filter_rule) > 0:
            request_url = request_url + '&' + self.filter_rule    
        self.request_headers['Referer'] = request_url
        self.request_headers['User-Agent'] = random.choice(self.request_user_agents)
        yield scrapy.Request(
            url=request_url, 
            headers=self.request_headers, 
            callback=self.parse_search_response, 
            meta={'proxy': random.choice(self.request_proxies), 'my_scrolls': self.max_file_number // 10})

    # Parse search results
    def parse_search_response(self, response):
        # For video without play list
        search_titles = response.css('a#video-title::text').extract()
        search_links = response.css('a#video-title::attr(href)').extract()
        self.logger.info(
            'search_titles=%s,search_links=%s' % 
            (len(search_titles), len(search_links)))
        # Encode item one by one
        self.logger.info('Ready to download %s titles and %s links' % (len(search_titles), len(search_links)))
        for title, link in zip(search_titles, search_links):
            if self.total_video_number >= self.max_file_number:
                self.logger.info('Downloaded number(%s) exceeds allowed number(%s)' % (self.total_video_number, self.max_file_number))
                break
            title = title.replace('\n', '').replace('\r', '').strip()
            if title is not None and len(title) > 0 and link is not None and len(link) > 0:
                # Encode video item
                self.total_video_number = self.total_video_number + 1
                video_item = YoutubeItem()
                video_item['type'] = 'summary'
                video_item['data'] = {
                    'source': 'Youtube',
                    'file_index': self.total_video_number,
                    'file_uuid': uuid.uuid3(uuid.NAMESPACE_DNS, title), 
                    'file_title': title, 
                    'file_link': 'https://www.youtube.com' + link}
                
                # Insert one item into database
                with self.db_connector.cursor() as cursor:
                    sql = ('REPLACE INTO tVideos('
                        'Source,Keywords,FileID,FileTitle,FileLink,LastUpdateTime) '
                        'VALUES(%s,%s,%s,%s,%s,CURRENT_TIMESTAMP());')
                    cursor.execute(
                        sql, 
                        (
                        str(video_item['data']['source']),
                        str(self.search_keywords), 
                        str(video_item['data']['file_uuid']), 
                        str(video_item['data']['file_title']), 
                        str(video_item['data']['file_link'])
                        )
                        )
                    self.db_connector.commit()
                
                # Yield item to download
                yield video_item
                
                # Yield video details request
                time.sleep(random.uniform(self.min_interval, self.max_interval))
                request_url = video_item['data']['file_link']
                self.request_headers['Referer'] = request_url
                self.request_headers['User-Agent'] = random.choice(self.request_user_agents)
                yield scrapy.Request(
                    url=request_url, 
                    headers=self.request_headers, 
                    callback = lambda response, file_uuid = video_item['data']['file_uuid'] : self.parse_details_response(response, file_uuid), 
                    meta={'proxy': random.choice(self.request_proxies), 'my_scrolls': 1})
        
        # For video with play list
        if self.total_video_number < self.max_file_number:
            view_more = response.css('#view-more>a::attr(href)').extract()
            for link in view_more:
                if self.total_video_number >= self.max_file_number:
                    self.logger.info('Downloaded number(%s) exceeds allowed number(%s)' % (self.total_video_number, self.max_file_number))
                    break
                request_url = 'https://www.youtube.com' + link
                self.request_headers['Referer'] = request_url
                self.request_headers['User-Agent'] = random.choice(self.request_user_agents)
                yield scrapy.Request(
                    url=request_url, 
                    headers=self.request_headers, 
                    callback=self.parse_view_more_response, 
                    meta={'proxy': random.choice(self.request_proxies), 'my_scrolls': self.max_file_number // 10})
                time.sleep(random.uniform(self.min_interval, self.max_interval))
            
    # Parse video in list 
    def parse_view_more_response(self, response):
        more_titles = response.css('span#video-title::text').extract()
        more_links = response.css('a#thumbnail::attr(href)').extract()
        self.logger.info('more_titles=%s,more_links=%s' % (len(more_titles), len(more_links)))
        # Encode item one by one
        self.logger.info('Ready to download %s titles and %s links' % (len(more_titles), len(more_links)))
        for title, link in zip(more_titles, more_links):
            if self.total_video_number >= self.max_file_number:
                self.logger.info('Downloaded number(%s) exceeds allowed number(%s)' % (self.total_video_number, self.max_file_number))
                break
            title = title.replace('\n', '').replace('\r', '').strip()
            if title is not None and len(title) > 0 and link is not None and len(link) > 0:
                # Encode item
                # https://www.youtube.com/watch?v=CxQ4aL5IXZw&list=PL1cd8z_9xX0_7riTNCD1rSJucfJjJ2hPs&index=1
                link = link.split('&')
                if link is not None and len(link) > 0:
                    link = link[0]
                    self.total_video_number = self.total_video_number + 1
                    video_item = YoutubeItem()
                    video_item['type'] = 'summary'
                    video_item['data'] = {
                        'source': 'Youtube',
                        'file_index': self.total_video_number,
                        'file_uuid': uuid.uuid3(uuid.NAMESPACE_DNS, title), 
                        'file_title': title, 
                        'file_link': 'https://www.youtube.com' + link}
                    
                    # Insert one item into database
                    with self.db_connector.cursor() as cursor:
                        sql = ('REPLACE INTO tVideos('
                            'Source,Keywords,FileID,FileTitle,FileLink,LastUpdateTime) '
                            'VALUES(%s,%s,%s,%s,%s,CURRENT_TIMESTAMP());')
                        cursor.execute(
                            sql, 
                            (
                            str(video_item['data']['source']),
                            str(self.search_keywords), 
                            str(video_item['data']['file_uuid']), 
                            str(video_item['data']['file_title']), 
                            str(video_item['data']['file_link'])
                            )
                            )
                        self.db_connector.commit()
                    
                    # Yield item to download
                    yield video_item
                    
                    # Yield video details request
                    time.sleep(random.uniform(self.min_interval, self.max_interval))
                    request_url = video_item['data']['file_link']
                    self.request_headers['Referer'] = request_url
                    self.request_headers['User-Agent'] = random.choice(self.request_user_agents)
                    yield scrapy.Request(
                        url=request_url, 
                        headers=self.request_headers, 
                        callback = lambda response, file_uuid = video_item['data']['file_uuid'] : self.parse_details_response(response, file_uuid), 
                        meta={'proxy': random.choice(self.request_proxies), 'my_scrolls': 1})
                
    # Parse details response
    def parse_details_response(self, response, file_uuid):
        # Visitor number
        visitor_number = '0'
        visitors = response.css('#count>yt-view-count-renderer>span.view-count.style-scope.yt-view-count-renderer::text').extract_first().strip()
        if visitors is not None:
            visitors = visitors.split()
            if visitors is not None and len(visitors) > 0:
                visitor_number = visitors[0]
        if visitor_number is None or len(visitor_number) <= 0 or 'no' == visitor_number.lower():
            visitor_number = '0'
        else:
            numbers = tuple(visitor_number.split(','))
            visitor_number = ''.join(numbers)
        # Comments
        likes_number = '0'
        dislikes_number = '0'
        comments = response.css('#text.style-scope.ytd-toggle-button-renderer.style-text::attr(aria-label)').extract()
        if comments is not None and len(comments) >= 2:
            likes = comments[0].split()
            if likes is not None and len(likes) > 0:
                likes_number = likes[0].replace('\n', '').strip()
            dislikes = comments[1].split()
            if dislikes is not None and len(dislikes) > 0:
                dislikes_number = dislikes[0].replace('\n', '').strip()
        if likes_number is None or len(likes_number) <= 0 or 'no' == likes_number.lower():
            likes_number = '0'
        else:
            numbers = tuple(likes_number.split(','))
            likes_number = ''.join(numbers)
        if dislikes_number is None or len(dislikes_number) <= 0 or 'no' == dislikes_number.lower():
            dislikes_number = '0'
        else:
            numbers = tuple(dislikes_number.split(','))
            dislikes_number = ''.join(numbers)
        # Channel
        channel_link = response.css('#owner-name>a::attr(href)').extract_first().replace('\n', '').strip()
        if channel_link is None or len(channel_link) <= 0:
            channel_link = ''
        channel_name = response.css('#owner-name>a::text').extract_first().replace('\n', '').strip()
        if channel_name is None or len(channel_name) <= 0:
            channel_name = ''
        channel_id = channel_link.split('/')
        if channel_id is not None and len(channel_id) > 0:
            channel_id = channel_id[-1]
        if channel_id is None or len(channel_id) <= 0:
            channel_id = '0'
        # Released time
        released_time = response.css('#upload-info>span::text').extract_first().replace('\n', '').strip()
        # Premiered Apr 12, 2019 / Published on Apr 10, 2019 / Published on Nov 24, 2018
        if released_time is not None and len(released_time) > 0:
            released_time = released_time.lower()
            released_year = 0
            released_month = 0
            released_day = 0
            for i in range(0, len(self.month_abbrs)):
                find_pos = released_time.find(self.month_abbrs[i])
                if find_pos >= 0:
                    # Published on Nov 24, 2018 ==> 10, 2019
                    released_time = released_time[find_pos + len(self.month_abbrs[i]): ]
                    if released_time is not None and len(released_time) > 0:
                        released_time = released_time.split(',')
                        if released_time is not None and len(released_time) >= 2:
                            released_year = int(released_time[1].strip())
                            released_month = i + 1
                            released_day = int(released_time[0].strip())
                    break;
            if released_year > 0 and released_month > 0 and released_day > 0:
                released_time = '{:04d}-{:02d}-{:02d}'.format(released_year, released_month, released_day)
            else:
                released_time = ''
        if released_time is None or len(released_time) <= 0:
            released_time = ''
        # Description
        discription = response.css('#description>yt-formatted-string::text').extract_first()
        if discription is not None and len(discription) > 0:
            discription = discription.strip()
        if discription is None or len(discription) <= 0:
            discription = ''
        
        # Yield details item        
        video_item = YoutubeItem()
        video_item['type'] = 'details'
        video_item['data'] = {
            'source': 'Youtube',
            'file_uuid': file_uuid, 
            'channel_id': channel_id,
            'channel_name': channel_name,
            'channel_link': 'https://www.youtube.com' + channel_link,
            'visitor_number': visitor_number, 
            'likes_number': likes_number,
            'dislikes_number': dislikes_number,
            'released_time': released_time,
            'discription': discription}
        # self.logger.info('video_details=%s' % video_item)
        yield video_item
