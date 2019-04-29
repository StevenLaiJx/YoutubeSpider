# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import os
import youtube_dl
import time

class YoutubePipeline(object):
    # Initalize parameters
    download_path = os.path.join(os.getcwd(), 'Download')
    if os.path.exists(download_path) is not True:
        os.mkdir(download_path)
    video_spider = None
    video_item = None
    video_exts = ['mpeg', 'mpg', 'mov', 'avi', 'wmv', 'wav', '3gp', 'mkv', 'rm', 'rmvb', 'webm', 'mp4']
    
    # Process pipeline item
    def process_item(self, item, spider):
        self.video_spider = spider
        self.video_item = item
        # Download video
        if 'summary' == item['type']:
            # Download options
            ydl_opts = {
                'format': 'best',
                'progress_hooks': [self.rename_hook],
                'outtmpl': '%(title)s%(ext)s',
                'proxy': '127.0.0.1:1080', 
                'socket_timeout': 120, 
                'prefer_ffmpeg': True,
                'logger': spider.logger}
            with youtube_dl.YoutubeDL(ydl_opts) as ydl:
                try:
                    spider.logger.info('Ready to download #%s video from %s' % (item['data']['file_index'], item['data']['file_link']))
                    result = ydl.download([item['data']['file_link']])
                    
                except Exception as e:
                    spider.logger.info('youtube_dl.YoutubeDL() raise exception, error: %s' % str(e))
        # Update database
        elif 'details' == item['type']:
            # Update details
            with self.video_spider.db_connector.cursor() as cursor:
                sql = ('UPDATE tVideos '
                    'SET CatalogID=%s,CatalogName=%s,CatalogLink=%s,VisitorNumber=%s,LikesNumber=%s,DislikesNumber=%s,'
                    'ReleasedTime=%s,Description=%s,LastUpdateTime=CURRENT_TIMESTAMP() '
                    'WHERE Source=%s AND Keywords=%s AND FileID=%s;')
                cursor.execute(
                    sql, 
                    (
                    str(self.video_item['data']['channel_id']),
                    str(self.video_item['data']['channel_name']),
                    str(self.video_item['data']['channel_link']),
                    str(self.video_item['data']['visitor_number']),
                    str(self.video_item['data']['likes_number']),
                    str(self.video_item['data']['dislikes_number']),
                    str(self.video_item['data']['released_time']),
                    str(self.video_item['data']['discription']),                    
                    str(self.video_item['data']['source']),
                    str(self.video_spider.search_keywords), 
                    str(self.video_item['data']['file_uuid'])
                    )
                    )
                self.video_spider.db_connector.commit()
            
                
    # Process rename hook
    def rename_hook(self, data):
        if data['status'] != 'finished':
            return

        lower_filename = data['filename'].lower()
        file_name = ''
        file_ext = ''
        for video_ext in self.video_exts:
            if lower_filename[-len(video_ext): ] == video_ext:
                file_name = data['filename'][0: -len(video_ext)]
                file_ext = video_ext
                break;
        if len(file_name) > 0 and len(file_ext) > 0:
            try:
                # Rename file
                self.video_item['data']['file_name'] = '{}.{}'.format(file_name, file_ext)
                file_path = os.path.join(self.download_path, self.video_spider.search_keywords)
                if os.path.exists(file_path) is not True:
                    os.mkdir(file_path)
                file_path = os.path.join(file_path, self.video_item['data']['file_name'])
                self.video_item['data']['file_path'] = file_path
                if os.path.exists(file_path) is True:
                    os.remove(file_path)
                os.rename(data['filename'], file_path)
                
                # Update database
                with self.video_spider.db_connector.cursor() as cursor:
                    sql = ('UPDATE tVideos '
                        'SET FileName=%s,FilePath=%s,LastUpdateTime=CURRENT_TIMESTAMP() '
                        'WHERE Source=%s AND Keywords=%s AND FileID=%s;')
                    cursor.execute(
                        sql, 
                        (
                        str(self.video_item['data']['file_name']), 
                        str(self.video_item['data']['file_path']),
                        str(self.video_item['data']['source']),
                        str(self.video_spider.search_keywords), 
                        str(self.video_item['data']['file_uuid'])
                        )
                        )
                    self.video_spider.db_connector.commit()
            except Exception as e:
                self.video_spider.logger.info('youtube_dl.progress_hooks() raise exception, error: %s' % str(e))