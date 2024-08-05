# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from sqlalchemy import create_engine
import redis
import os

class LesionesTrasnfermarketPipeline:
    def process_item(self, item, spider):
        return item

class PipelineClubs:
    def __init__(self):
        self.r = redis.StrictRedis(host='redis', port=6379, db=0)
        self.group_name = os.getenv('GROUP_NAME', 'main_spider_redis')
        self.total_count = int(self.r.get(f'{self.group_name}:total_count') or 0)
        self.engine = create_engine('mssql+pyodbc://scraper:scraper1234A@db-container:1433/tfm_lesiones?driver=ODBC+Driver+17+for+SQL+Server')

    def process_item(self, item, spider):
        df_actual = item.get('dataframe')
        if not df_actual.empty:
            df_actual.to_sql(name=item.get('tipo'), con=self.engine, index=False, if_exists='append')


    def close_spider(self, spider):
        completed_count = self.r.incr(f'{self.group_name}:completed_count')
        if completed_count == self.total_count:
            self.r.delete(f'{self.group_name}:lock')

