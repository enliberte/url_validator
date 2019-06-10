# -*- coding: utf-8 -*-
import sqlite3
from .items import Link


class SQLitePipeline:
    create_rqst = """
    CREATE TABLE IF NOT EXISTS {}
    (
    text TEXT NOT NULL,
    url TEXT NOT NULL,
    referer_url TEXT NOT NULL,
    status_code INT NOT NULL
    )
    """

    clear_rqst = """
    DELETE FROM {}
    """

    insert_rqst = """
    INSERT OR REPLACE INTO {}
    values (?, ?, ?, ?)
    """

    def __init__(self, path, table):
        self.path = path
        self.table = table
        self.db = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(path=crawler.spider.db_path, table=crawler.settings.get('SQLITE_TABLE'))

    def open_spider(self, spider):
        self.db = sqlite3.connect(self.path)
        self.db.execute(self.create_rqst.format(self.table))
        self.db.execute(self.clear_rqst.format(self.table))

    def close_spider(self, spider):
        if self.db:
            self.db.close()

    def process_item(self, item, spider):
        if isinstance(item, Link):
            self.db.execute(self.insert_rqst.format(self.table),
                            (
                                item['text'],
                                item['url'],
                                item['referer_url'],
                                item['status_code'])
                            )
            self.db.commit()
