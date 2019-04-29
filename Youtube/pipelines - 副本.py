# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import os
import youtube_dl
import time
import uuid

class YoutubePipeline(object):
    download_path = os.path.join(os.getcwd(), 'Download')
    if os.path.exists(download_path) is not True:
        os.mkdir(download_path)
    video_spider = None
    video_item = None
    video_exts = ['mpeg', 'mpg', 'mov', 'avi', 'wmv', 'wav', '3gp', 'mkv', 'rm', 'rmvb', 'webm', 'mp4']
    
    # Process pipeline item
    def process_item(self, item, spider):
        # spider.logger.info('max_file_size=%s,type=%s,data=%s' % (spider.max_file_size, item['type'], item['data']))
        # Download options
        item['data']['timestamp'] = int(time.time())
        item['data']['fileuuid'] = uuid.uuid3(uuid.NAMESPACE_DNS, item['data']['title'])
        self.video_spider = spider
        self.video_item = item
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
                # Get video information
                # spider.logger.info('video_link=%s' % item['data']['link'])
                # video_informations = ydl.extract_info(item['data']['link'], download=False)
                # video_formats = video_informations.get('formats')
                # spider.logger.info('video_informations=%s' % video_informations)
                
                '''
                # Get best video/audio with higest resolution
                best_audio = {'format_id': '0', 'filesize': 0}
                best_video = {'format_id': '0', 'filesize': 0}
                for video_format in video_formats:
                    format_id = video_format.get('format_id')
                    vcodec = video_format.get('vcodec')
                    acodec = video_format.get('acodec')
                    file_size = video_format.get('filesize')
                    # spider.logger.info('format_id=%s,file_size=%s,acodec=%s,vcodec=%s' % (format_id, file_size, acodec, vcodec))
                    if format_id is None or len(format_id) <= 0 or file_size is None or int(file_size) <= 0:
                        continue
                    if int(file_size) > spider.max_file_size:
                        continue
                    if acodec is not None and len(acodec) > 0 and 'none' != acodec.lower():
                        if best_audio['filesize'] < file_size:
                            best_audio['format_id'] = format_id
                            best_audio['filesize'] = file_size
                    if vcodec is not None and len(vcodec) > 0 and 'none' != vcodec.lower():
                        if best_video['filesize'] < file_size:
                            best_video['format_id'] = format_id
                            best_video['filesize'] = file_size
                
                # Download best video/audio with higest resolution
                if len(best_video['format_id']) > 0 and int(best_video['format_id']) > 0 and len(best_audio['format_id']) > 0 and int(best_audio['format_id']) > 0:
                    best_resolution = best_video['format_id'] + '+' + best_audio['format_id']
                    spider.logger.info('best_resolution=%s' % best_resolution)
                    ydl.params = {'format': best_resolution}
                    ydl.process_video_result(video_informations)
                '''
                result = ydl.download([item['data']['link']])
                # ydl.process_video_result(video_informations)
                
            except Exception as e:
                spider.logger.info('youtube_dl.YoutubeDL() raise exception, error: %s' % str(e))
                
    # Process rename hook
    def rename_hook(self, data):
        # self.video_spider.logger.info('video_data=%s' % data)
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
            self.video_item['data']['filename'] = '{}.{}'.format(file_name, file_ext)
            file_path = os.path.join(self.download_path, self.video_spider.search_keywords)
            if os.path.exists(file_path) is not True:
                os.mkdir(file_path)
            file_path = os.path.join(file_path, self.video_item['data']['filename'])
            self.video_item['data']['filepath'] = file_path
            if os.path.exists(file_path) is True:
                os.remove(file_path)
            os.rename(data['filename'], file_path)
            with self.video_spider.db_connector.cursor() as cursor:
                sql = ('REPLACE INTO tDownloaded('
                    'Keywords,FileID,FileName,FilePath,FileTitle,URLLink,LastUpdateTime) '
                    'VALUES(%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP());')
                cursor.execute(
                    sql, 
                    (str(self.video_spider.search_keywords), 
                    str(self.video_item['data']['fileuuid']), 
                    str(file_name), 
                    str(file_path), 
                    str(self.video_item['data']['title']), 
                    str(self.video_item['data']['link']))
                    )
                self.video_spider.db_connector.commit()
            self.video_spider.logger.info('Downloading finished, file path=%s' % self.video_item['data']['filepath'])
