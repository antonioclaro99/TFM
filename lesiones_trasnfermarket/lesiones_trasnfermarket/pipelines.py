# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pandas as pd


class LesionesTrasnfermarketPipeline:
    def process_item(self, item, spider):
        return item

class PipelineClubs:
    def __init__(self):
        self.df_global_clubs = pd.DataFrame()
        self.df_global_jugadores = pd.DataFrame()
        self.df_global_lesiones = pd.DataFrame()
        self.df_global_estadisticas = pd.DataFrame()

    def process_item(self, item, spider):
        df_actual = item.get('dataframe')
        if item.get('tipo') == 'clubs':
            self.df_global_clubs = pd.concat([self.df_global_clubs, df_actual], ignore_index=True)

        elif item.get('tipo') == 'jugador':
            self.df_global_jugadores = pd.concat([self.df_global_jugadores, df_actual], ignore_index=True)

        elif item.get('tipo') == 'lesiones':
            self.df_global_lesiones = pd.concat([self.df_global_lesiones, df_actual], ignore_index=True)

        elif item.get('tipo') == 'estadisticas':
            self.df_global_estadisticas = pd.concat([self.df_global_estadisticas, df_actual], ignore_index=True)

        # return df_actual
    
    def close_spider(self, spider):
        self.df_global_clubs.to_csv('data/clubs/clubs.csv', index=True)
        self.df_global_jugadores.to_csv('data/jugadores/jugadores.csv', index=True)
        self.df_global_lesiones.to_csv('data/lesiones/lesiones.csv', index=True)
        self.df_global_estadisticas.to_csv('data/estadisticas/estadisticas.csv', index=True)