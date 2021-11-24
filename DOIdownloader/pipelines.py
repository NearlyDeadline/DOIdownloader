# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import traceback
from typing import List
import logging
import re
import json
import pymysql
from pymysql.converters import escape_string
from scrapy.exceptions import DropItem


class Author:
    full_name = ''
    university = ''
    contribution = ''
    ieee_id = ''
    is_first_author = False


class DoidownloaderPipeline:
    IEEEpattern = 'xplGlobal.document.metadata=\{.*\};'

    def __init__(self):
        self.__connection_config = json.load(open('./config.json'))

    def open_spider(self, spider):
        self.__conn = pymysql.connect(user=self.__connection_config['user'],
                                      password=self.__connection_config['password'],
                                      host=self.__connection_config['host'],
                                      database=self.__connection_config['database'],
                                      charset='utf8mb4')
        self.__cursor = self.__conn.cursor()

    def close_spider(self, spider):
        self.__cursor.close()
        self.__conn.close()

    def init_author_list(self, content) -> List[Author]:
        author_list = []
        for i in content['authors']:
            author = Author()
            author.full_name = i['name']
            try:
                segs = i['affiliation'][0].split(',')
                if len(segs) < 4:
                    # 4：学院,大学,城市,国家。小于4说明缺了学院，大学第一个
                    author.university = segs[0]
                else:
                    author.university = segs[1]
            except IndexError:
                author.university = ''
            author_list.append(author)
        author_list[0].is_first_author = True
        return author_list

    def process_item(self, item, spider):
        item_type = item['type']
        paper_id = item['paper_id']
        if item_type == 'IEEE':
            try:
                data = re.search(self.IEEEpattern, item['content'])
                meta_data = data.group()
                content = json.loads(meta_data[len('xplGlobal.document.metadata='): -1])
                author_list = self.init_author_list(content)
                for author in author_list:
                    sql = f"SELECT id, rid FROM author WHERE ieee_id = {author.ieee_id};"
                    self.__cursor.execute(sql)
                    result = self.__cursor.fetchone()

                    if not result:
                        sql = f"INSERT INTO researcher(`name`) VALUES ('{escape_string(author.full_name)}');"
                        self.__cursor.execute(sql)
                        self.__conn.commit()

                        self.__cursor.execute('SELECT last_insert_id();')
                        researcher_id = self.__cursor.fetchone()[0]

                        sql = f"INSERT INTO author(rid, ieee_id, university) VALUES('{researcher_id}', '{author.ieee_id}', '{escape_string(author.university)}');"
                        self.__cursor.execute(sql)
                        self.__conn.commit()

                        self.__cursor.execute('SELECT last_insert_id();')
                        author_id = self.__cursor.fetchone()[0]
                    else:
                        author_id = result[0]
                        researcher_id = result[1]

                    sql = f"INSERT INTO author_paper(aid, pid, contribution) VALUES('{author_id}', '{paper_id}', '{'FIRST_AUTHOR' if author.is_first_author else 'PAPER_AUTHOR'}');"
                    self.__cursor.execute(sql)
                    self.__conn.commit()
            except Exception as e:
                logging.error(f"发生类型为{type(e)}的错误：'{repr(e)}'。请检查pid={paper_id}。追踪位置：{traceback.format_exc()}。")
                raise DropItem()
        return item
