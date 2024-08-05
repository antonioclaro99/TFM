import scrapy
import pandas as pd
import time
from scrapy_redis.spiders import RedisSpider
import json

from lesiones_trasnfermarket.items import RedisItem
from scrapy_redis.connection import get_redis_from_settings

from scrapy.utils.project import get_project_settings
import redis
from datetime import datetime



import requests
from sqlalchemy import create_engine,text, MetaData, Table, Column, Integer, LargeBinary, String
from sqlalchemy.orm import sessionmaker
import os

from traceback import format_exc

import logging


# Configura el logging para la salida en consola
logging.basicConfig(
    level=logging.DEBUG,  # Puedes cambiar a INFO o WARNING según lo necesites
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Esto enviará los logs a la consola
    ]
)



class MainSpider(RedisSpider):
    name = 'main_spider_redis'
    redis_key = 'main_spider_redis:items'
    redis_batch_size = 1
    hostname = os.getenv('HOSTNAME')

    # Max idle time(in seconds) before the spider stops checking redis and shuts down
    max_idle_time = 120

    redis_conn = redis.StrictRedis(host='redis', port=6379, db=0)
    # Crear la sesión
    engine = create_engine('mssql+pyodbc://scraper:scraper1234A@db-container:1433/tfm_lesiones?driver=ODBC+Driver+17+for+SQL+Server')
    Session = sessionmaker(bind=engine)
    session = Session()

    equipos_a_scrapear_tomados=False
    clubs_to_scrape = None

            # Accede a los parámetros configurados
    settings = get_project_settings()
    min_season = settings.get('MIN_SEASON', 2010)
    max_season = settings.get('MAX_SEASON', 2024)
    current_season = settings.get('CURRENT_SEASON', 2024)

    incremental = settings.get('INCREMENTAL', True)
    continentes_a_incluir = settings.get('CONTINENTS_TO_INCLUDE', [])
    paises_a_incluir = settings.get('COUNTRIES_TO_INCLUDE', [])
    ligas_a_incluir = settings.get('LEAGUES_TO_INCLUDE', [])
    clubs_a_incluir = settings.get('CLUBS_TO_INCLUDE', [])


    seasons = [i for i in range(min_season,max_season+1)]

    BASE_URL = 'https://www.transfermarkt.es'

    CONINENTES_URLS = ["https://www.transfermarkt.es/wettbewerbe/europa/wettbewerbe?plus=1",
                      "https://www.transfermarkt.es/wettbewerbe/asien/wettbewerbe?plus=1",
                      "https://www.transfermarkt.es/wettbewerbe/amerika/wettbewerbe?plus=1",
                      "https://www.transfermarkt.es/wettbewerbe/afrika/wettbewerbe?plus=1"]
            

    # XPATH PAIS
    XPATH_ENLACES_PAISES = '//map/area/@href'
    XPATH_NOMBRE_PAISES = '//map/area/@title'
    URL_BANDERA_PAIS = f'https://tmssl.akamaized.net/images/flagge/tiny/189.png?lm=1520611569'


    XPATH_CELDAS_LIGAS = '//*[@id="yw1"]/table/tbody/tr[position()>1]/td'

    # XPATH LIGAS
    XPATH_ENLACES_LIGAS = '//table[@class="inline-table"]//tr[1]/td[2]/a/@href'
    XPATH_NOMBRE_LIGAS = '//table[@class="inline-table"]//tr[1]/td[2]/a/@title'
    XPATH_ENLACES_IMG_LIGAS = '//table[@class="inline-table"]//tr[1]/td[1]/a/img/@src'
    XPATH_PAIS_LIGA = f'{XPATH_CELDAS_LIGAS}[2]/img/@title'
    XPATH_ENLACE_BANDERA_PAIS_LIGA = f'{XPATH_CELDAS_LIGAS}[2]/img/@src'
    XPATH_NUM_CLUBES_LIGA = f'{XPATH_CELDAS_LIGAS}[3]/text()'
    XPATH_NUM_JUGADORES_LIGA = f'{XPATH_CELDAS_LIGAS}[4]/text()'
    XPATH_EDAD_MEDIA_LIGA = f'{XPATH_CELDAS_LIGAS}[5]/text()'
    XPATH_PORCENTAJE_EXTRANJEROS_LIGA = f'{XPATH_CELDAS_LIGAS}[6]//text()'
    XPATH_GOLES_POR_ENCUENTRO_LIGA = f'{XPATH_CELDAS_LIGAS}[8]//text()'
    XPATH_VALOR_DE_MERCADO_MEDIO_LIGA = f'{XPATH_CELDAS_LIGAS}[10]/text()'
    XPATH_VALOR_DE_MERCADO_TOTAL_LIGA = f'{XPATH_CELDAS_LIGAS}[11]/text()'


    # XPATH CLUBS
    XPATH_ENLACES_EQUIPOS = '//*[@id="yw1"]/table/tbody/tr/td[2]/a[1]/@href'
    XPATH_NOMBRE_CLUBS = '//*[@id="yw1"]/table/tbody/tr/td[2]/a[1]/text()'
    XPATH_ENLACES_ESCUDOS = '//*[@id="yw1"]/table/tbody/tr/td[1]/a/img/@src'
    XPATH_EDAD_MEDIA = '//*[@id="yw1"]/table/tbody/tr/td[4]/text()'
    XPATH_VALOR_MERCADO = '//*[@id="yw1"]/table/tbody/tr/td[7]/a/text()'

    # XPATH JUGADORES
    XAPTH_CELDAS = '//*[@id="yw1"]/table/tbody/tr/td'

    # XPATH_ENLACES_JUGADORES = '//*[@id="yw1"]/table/tbody/tr/td[2]/table/tbody/tr[1]/td[2]/a/@href'
    XPATH_ENLACES_JUGADORES = '//table[@class="inline-table"]//tr[1]/td[2]/a/@href'
    # XPATH_NOMBRE_JUGADORES = '//*[@id="yw1"]/table/tbody/tr/td[2]/table/tbody/tr[1]/td[2]/a/text()'
    XPATH_FOTO_JUGADORES = '//table[@class="inline-table"]//tr[1]/td[1]/img/@data-src'
    XPATH_NOMBRE_JUGADORES = '//table[@class="inline-table"]//tr[1]/td[2]/a/text()'
    XPATH_FECHA_NACIMIENTO_EDAD = '//*[@id="yw1"]/table/tbody/tr/td[3]/text()'
    XPATH_PAIS = '//*[@id="yw1"]/table/tbody/tr/td[4]/img[1]/@src'
    XPATH_ALTURA = '//*[@id="yw1"]/table/tbody/tr/td[5]/text()'
    XPATH_ENLACES_BANDERAS = '//*[@id="yw1"]/table/tbody//td[4]/img[1]/@src'
    XPATH_PIE = '//*[@id="yw1"]/table/tbody/tr/td[6]'
    XPATH_FECHA_FICHAJE = '//*[@id="yw1"]/table/tbody/tr/td[7]'
    XPATH_CLUB_ANTERIOR_ID = '//*[@id="yw1"]/table/tbody/tr/td[8]'
    XPATH_VALOR_MERCADO_JUGADOR = '//*[@id="yw1"]/table/tbody/tr/td[10]//text()'
    XPATH_POSICION = '//table[@class="inline-table"]//tr[2]/td[1]/text()'

    XPATH_CLUB_ANTERIOR_ID_RELATIVE = './a/@href'


    # XPATH_JUGADOR_TEMPORADAS_ANTERIORES

    XPATH_ALTURA_TEMP_ANTERIORES = '//*[@id="yw1"]/table/tbody/tr/td[6]/text()'
    XPATH_PIE_TEMP_ANTERIORES = '//*[@id="yw1"]/table/tbody/tr/td[7]'
    XPATH_FECHA_FICHAJE_TEMP_ANTERIORES = '//*[@id="yw1"]/table/tbody/tr/td[8]'
    XPATH_CLUB_ANTERIOR_ID_TEMP_ANTERIORES = '//*[@id="yw1"]/table/tbody/tr/td[9]'
    XPATH_VALOR_MERCADO_JUGADOR_TEMP_ANTERIORES = '//*[@id="yw1"]/table/tbody/tr/td[10]//text()'



    start_items = [ {"url": "https://www.transfermarkt.es/wettbewerbe/europa/wettbewerbe?plus=1", "meta": {"metodo": "parse_continente_paises"}},
                   {"url": "https://www.transfermarkt.es/wettbewerbe/amerika/wettbewerbe?plus=1", "meta": {"metodo": "parse_continente_paises"}},
                   {"url": "https://www.transfermarkt.es/wettbewerbe/asien/wettbewerbe?plus=1", "meta": {"metodo": "parse_continente_paises"}},
                   {"url": "https://www.transfermarkt.es/wettbewerbe/afrika/wettbewerbe?plus=1", "meta": {"metodo": "parse_continente_paises"}},
                   {"url": "https://www.transfermarkt.es/wettbewerbe/europa/wettbewerbe?plus=1", "meta": {"metodo": "parse_continente_competiciones"}},
                   {"url": "https://www.transfermarkt.es/wettbewerbe/amerika/wettbewerbe?plus=1", "meta": {"metodo": "parse_continente_competiciones"}},
                   {"url": "https://www.transfermarkt.es/wettbewerbe/asien/wettbewerbe?plus=1", "meta": {"metodo": "parse_continente_competiciones"}},
                   {"url": "https://www.transfermarkt.es/wettbewerbe/afrika/wettbewerbe?plus=1", "meta": {"metodo": "parse_continente_competiciones"}},
                   {"url": "https://www.transfermarkt.es/wettbewerbe/europa/wettbewerbe?plus=1&page=2", "meta": {"metodo": "parse_continente_competiciones"}},
                   {"url": "https://www.transfermarkt.es/wettbewerbe/europa/wettbewerbe?plus=1&page=3", "meta": {"metodo": "parse_continente_competiciones"}}]
    






    def __init__(self, *args, **kwargs):
        super(MainSpider, self).__init__(*args, **kwargs)
        print(f"{self.hostname}")
        logging.debug(f"Instancia {self.hostname} inicializada.")
        

        self.instance_type = os.getenv('INSTANCE_TYPE', 'regular')


        if self.instance_type=="rw":
                
                # queries para borrar datos de tablas no incrementales
                
                delete_query_paises = " TRUNCATE TABLE dbo.paises"
                delete_query_ligas = " TRUNCATE TABLE dbo.ligas"
                self.session.execute(text(delete_query_paises))

                self.session.commit()

                self.session.execute(text(delete_query_ligas))

                self.session.commit()


                # Borrar clubes de la tabla anterior de ejecuciones anteriores
                delete_clubs_tabla_aux = f"delete from dbo.clubs_to_scrape where 1=1"

                self.session.execute(text(delete_clubs_tabla_aux))
                self.session.commit()

                if self.incremental:
                    query_clubs_a_borrar = None
                    if self.continentes_a_incluir:
                        continentes_query_list = "'"+"', '".join(map(str, self.continentes_a_incluir))+"'"

                        query_clubs_a_borrar = f"""select id_club_tfmk from dbo.clubs where id_liga_tfmk in 
                                                        (
                                                            select id_liga from dbo.ligas where id_pais in 
                                                            (
                                                                select id_pais from dbo.paises where continente in ({continentes_query_list})
                                                            )
                                                        )"""


                    elif self.paises_a_incluir:

                        countries_query_list = "'"+"', '".join(map(str, self.paises_a_incluir))+"'"

                        query_clubs_a_borrar = f"""select id_club_tfmk from dbo.clubs where id_liga_tfmk in 
                                                        (
                                                            select id_liga from dbo.ligas where id_pais in 
                                                            (
                                                                select id_pais from dbo.paises where nombre in ({countries_query_list})
                                                            )
                                                        )"""

                    elif self.ligas_a_incluir:
                        leagues_query_list = "'"+"', '".join(map(str, self.ligas_a_incluir))+"'"

                        query_clubs_a_borrar = f"""select id_club_tfmk from dbo.clubs where id_liga_tfmk in 
                                                        (
                                                            select id_liga from dbo.ligas where nombre in ({leagues_query_list})            
                                                        )"""

                    elif self.clubs_a_incluir:
                        clubs_query_list = "'"+"', '".join(map(str, self.clubs_a_incluir))+"'"

                        query_clubs_a_borrar = f"""select distinct id_club_tfmk from dbo.clubs where nombre_club in ({clubs_query_list})"""

                    
                    if query_clubs_a_borrar:

                        insert_clubs_tabla_aux = f"insert into dbo.clubs_to_scrape (id_club_tfmk) select distinct id_club_tfmk from dbo.clubs where id_club_tfmk in ({query_clubs_a_borrar} and temporada ={self.current_season})"

                        logging.debug(insert_clubs_tabla_aux)

                        self.session.execute(text(insert_clubs_tabla_aux))
                        query_jugadores_a_borrar = f"select id_jugador from dbo.jugador where id_temporada = {self.current_season} and id_club in ({query_clubs_a_borrar})"
                        
                        
                        delete_query_jugadores = f"delete from dbo.jugador where id_temporada = {self.current_season} and id_club in ({query_clubs_a_borrar})"
                        delete_query_clubs = f"delete from dbo.clubs where temporada = {self.current_season}"
                        delete_query_lesiones = f"delete from dbo.lesiones where id_jugador in ({query_jugadores_a_borrar})"
                        delete_query_estadisticas = f"delete from dbo.estadisticas_generales where temporada = {self.current_season} and id_jugador in ({query_jugadores_a_borrar})"
                    else:
                        query_jugadores_a_borrar = f"select id_jugador from dbo.jugador where id_temporada = {self.current_season}"
                        delete_query_jugadores = f"delete from dbo.jugador where id_temporada = {self.current_season}"
                        delete_query_clubs = f"delete from dbo.clubs where temporada = {self.current_season}"
                        delete_query_lesiones = f"delete from dbo.lesiones where id_jugador in ({query_jugadores_a_borrar})"
                        delete_query_estadisticas = f"delete from dbo.estadisticas_generales where temporada = {self.current_season} and id_jugador in ({query_jugadores_a_borrar})"

                    delete_queries = [delete_query_jugadores,delete_query_clubs,delete_query_lesiones,delete_query_estadisticas]
                    
                    for delete_query in delete_queries:
                        self.session.execute(text(delete_query))

                    self.session.commit()


                for start_item in self.start_items:
                    item = start_item
                    item_json = json.dumps(item)
                    self.redis_conn.lpush(self.redis_key, item_json)
    

    def spider_closed(self, spider, reason):
        self.session.close()

    def parse(self, response):

        metodo = response.meta.get('metodo')

        try:
            if metodo=="parse_continente_paises":
                continente = response.url.split("/")[-2]

                if continente == "europa":
                    continente = "Europa"
                elif continente == "asien":
                    continente = "Asia/Oceanía"
                elif continente == "afrika":
                    continente = "África"
                else:
                    continente = "América"

                enlaces_pais = response.xpath(self.XPATH_ENLACES_PAISES).extract()

                nombre_pais = response.xpath(self.XPATH_NOMBRE_PAISES).extract()

                df_pais = pd.DataFrame()

                df_pais["id_pais"] = [enlace.split("/")[-1] for enlace in enlaces_pais]
                df_pais["nombre"] = nombre_pais
                df_pais["continente"] = continente
                df_pais["enlace_bandera_pais"] = [f'https://tmssl.akamaized.net/images/flagge/medium/{id_pais}.png?lm=1520611569' for id_pais in df_pais["id_pais"]]
                df_pais["enlace_pais"] = enlaces_pais

                yield {'dataframe': df_pais, 'tipo': 'paises', 'pipeline':'PipelineClubs'}

            if metodo== "parse_continente_competiciones":

                continente = response.url.split("/")[-2]

                if continente == "europa":
                    continente = "Europa"
                elif continente == "asien":
                    continente = "Asia/Oceanía"
                elif continente == "afrika":
                    continente = "África"
                else:
                    continente = "América"

                enlaces = response.xpath(self.XPATH_ENLACES_LIGAS).extract()

                nombre_liga = response.xpath(self.XPATH_NOMBRE_LIGAS).extract()
                
                enlace_img_liga = response.xpath(self.XPATH_ENLACES_IMG_LIGAS).extract()

                pais = response.xpath(self.XPATH_PAIS_LIGA).extract()

                enlace_bandera = response.xpath(self.XPATH_ENLACE_BANDERA_PAIS_LIGA).extract()

                num_clubes = response.xpath(self.XPATH_NUM_CLUBES_LIGA).extract()

                num_jugadores = response.xpath(self.XPATH_NUM_JUGADORES_LIGA).extract()

                edad_media = response.xpath(self.XPATH_EDAD_MEDIA_LIGA).extract()

                porcentaje_extranjeros = response.xpath(self.XPATH_PORCENTAJE_EXTRANJEROS_LIGA).extract()

                # goles_por_partido = response.xpath(self.XPATH_GOLES_POR_ENCUENTRO_LIGA).extract()

                # valor_de_mercado_medio = response.xpath(self.XPATH_VALOR_DE_MERCADO_MEDIO_LIGA).extract()

                # valor_de_mercado_total = response.xpath(self.XPATH_VALOR_DE_MERCADO_TOTAL_LIGA).extract()

                df_ligas = pd.DataFrame()

                df_ligas["id_liga"] = [enlace.split("/")[-1] for enlace in enlaces]
                df_ligas["url_tfmk"] = enlaces
                df_ligas["nombre"] = nombre_liga
                df_ligas["enlace_img_liga"] = enlace_img_liga
                df_ligas["id_pais"] = [enlace.split("/")[-1].split(".")[0] for enlace in enlace_bandera]
                df_ligas["num_clubes"] = num_clubes
                df_ligas["num_jugadores"] = num_jugadores
                df_ligas["edad_media"] = [float(edad.replace(",",".")) if edad!='-' else 25.0 for edad in edad_media]

                df_ligas["porcentaje_extranjeros"] = porcentaje_extranjeros
                # df_ligas["goles_por_partido"] = [float(goles.replace(",",".")) for goles in goles_por_partido]
                # df_ligas["valor_de_mercado_medio"] = [float(vm.split(" ")[0].replace(",","000.")) if "mil mill." in vm else float(vm.split(" ")[0].replace(",",".")) for vm in valor_de_mercado_medio]
                # df_ligas["valor_de_mercado_total"] = [float(vm.split(" ")[0].replace(",","000.")) if "mil mill." in vm else float(vm.split(" ")[0].replace(",",".")) for vm in valor_de_mercado_total]
            
                yield {'dataframe': df_ligas, 'tipo': 'ligas','pipeline': 'PipelineClubs'}


                for url_liga,nombre_liga,pais_liga in zip(df_ligas["url_tfmk"],df_ligas["nombre"],pais):
                        scrape_clubs_in_league = "no"
                        if (nombre_liga in self.ligas_a_incluir or continente in self.continentes_a_incluir or pais_liga in self.paises_a_incluir) or (not self.ligas_a_incluir and not self.paises_a_incluir and not self.continentes_a_incluir):
                            scrape_clubs_in_league = "yes"
                        for season in self.seasons:
                            url_ligas = f"{self.BASE_URL}{url_liga}/plus/1?saison_id={season}"

                            item = {"url": url_ligas, "meta": {"metodo": "parse_liga","scrape_clubs_in_league": scrape_clubs_in_league }}

                            item_json = json.dumps(item)

                            self.redis_conn.lpush(self.redis_key, item_json)
                       
            elif metodo == "parse_liga":

                scrape_clubs_in_league = response.meta.get('scrape_clubs_in_league',"no")

                codigo_liga = response.url.split("/")[6]

                temporada = response.url.split("/")[-1].split("=")[-1]

                enlaces = response.xpath(self.XPATH_ENLACES_EQUIPOS).extract()
                
                nombre_clubs = response.xpath(self.XPATH_NOMBRE_CLUBS).extract()

                enlaces_escudos = response.xpath(self.XPATH_ENLACES_ESCUDOS).extract()

                edad_media = response.xpath(self.XPATH_EDAD_MEDIA).extract()

                valor_mercado = response.xpath(self.XPATH_VALOR_MERCADO).extract()

                df_clubs = pd.DataFrame()

                df_clubs["id_club_tfmk"] = [enlace.split("/")[-3] for enlace in enlaces]
                df_clubs["nombre_club"] = nombre_clubs
                df_clubs["url_club_tfmk"] = enlaces
                df_clubs["nombre_club_tfmk"] = [enlace.split("/")[1] for enlace in enlaces]
                df_clubs["escudo_club"] = [enlace_escudo.replace("tiny","big") for enlace_escudo in enlaces_escudos]
                df_clubs["edad_media_club"] = [float(edad.replace(",",".")) if edad!='-' else 25.0 for edad in edad_media]
                df_clubs["valor_mercado_millones_euros_club"] = [float(vm.replace("-","0").split(" ")[0].replace(",","000.")) if "mil mill." in vm else float(vm.replace("-","0").split(" ")[0].replace(",",".")) for vm in valor_mercado]
                df_clubs["id_liga_tfmk"] = codigo_liga
                df_clubs["temporada"] = temporada

                yield {'dataframe': df_clubs, 'tipo': 'clubs','pipeline': 'PipelineClubs'}

                if self.equipos_a_scrapear_tomados==False and self.incremental==True:
                    

                    query_clubs_to_scrape = """SELECT id_club_tfmk FROM tfm_lesiones.dbo.clubs_to_scrape;"""
                    self.clubs_to_scrape_str = pd.read_sql(query_clubs_to_scrape,self.engine)["id_club_tfmk"].to_list()
                    self.clubs_to_scrape_int = [int(club_id) for club_id in pd.read_sql(query_clubs_to_scrape,self.engine)["id_club_tfmk"].to_list()]

                    self.equipos_a_scrapear_tomados=True


                    
                for url_club,id_club,nombre_club in zip(df_clubs["url_club_tfmk"],df_clubs["id_club_tfmk"],df_clubs["nombre_club"]):
                    # logging.debug(f"URL: {str(response.url)}")
                    # logging.debug(f"scrape_clubs_in_league: {str(scrape_clubs_in_league)}")
                    # logging.debug(f"self.incremental==False and (not self.clubs_a_incluir) and scrape_clubs_in_league=='yes'")
                    # logging.debug(f"{str(self.incremental==False and (not self.clubs_a_incluir) and scrape_clubs_in_league=='yes')}")

                    # condition = ((self.incremental==False and (not self.clubs_a_incluir) and scrape_clubs_in_league=="yes") or (self.incremental==False and self.clubs_a_incluir and (nombre_club in self.clubs_a_incluir)) or (self.incremental==True and ((id_club in self.clubs_to_scrape_int) or (id_club in self.clubs_to_scrape_str) or (nombre_club in self.clubs_a_incluir))))
                    # logging.debug(f"Condicion larga :  {str(condition)}")

                    
                    if (self.incremental==False and (not self.clubs_a_incluir) and scrape_clubs_in_league=="yes") or (self.incremental==False and self.clubs_a_incluir and nombre_club in self.clubs_a_incluir) or (self.incremental==True and (id_club in self.clubs_to_scrape_int or id_club in self.clubs_to_scrape_str or nombre_club in self.clubs_a_incluir)) :
                        
                        if self.incremental==False:
                            query_insert_clubs_to_scrape = f"insert into dbo.clubs_to_scrape values ('{id_club}')"
                            self.session.execute(text(query_insert_clubs_to_scrape))
                            self.session.commit()


                        url = f"{self.BASE_URL}{url_club}/plus/1".replace("/startseite/","/kader/")
                        # url = f"{self.BASE_URL}{url_club}/saison_id/{temporada}/plus/1".replace("/startseite/","/kader/")

                        item = {"url": url, "meta": {"metodo": "parse_jugadores"}}

                        item_json = json.dumps(item)

                        self.redis_conn.lpush(self.redis_key, item_json)

            elif metodo == "parse_jugadores":

                codigo_club = response.url.split("/")[6]

                temporada = response.url.split("/")[-3]

                temporada_actual = self.settings.get('CURRENT_SEASON', 2024)

                if temporada==str(temporada_actual):
                    alturas = response.xpath(self.XPATH_ALTURA).extract()

                    pie = response.xpath(self.XPATH_PIE)

                    fecha_fichaje = response.xpath(self.XPATH_FECHA_FICHAJE)

                    club_anterior = response.xpath(self.XPATH_CLUB_ANTERIOR_ID)

                    valor_mercado = response.xpath(self.XPATH_VALOR_MERCADO_JUGADOR).extract()


                else:

                    alturas = response.xpath(self.XPATH_ALTURA_TEMP_ANTERIORES).extract()

                    pie = response.xpath(self.XPATH_PIE_TEMP_ANTERIORES)

                    fecha_fichaje = response.xpath(self.XPATH_FECHA_FICHAJE_TEMP_ANTERIORES)

                    club_anterior = response.xpath(self.XPATH_CLUB_ANTERIOR_ID_TEMP_ANTERIORES)

                    valor_mercado = response.xpath(self.XPATH_VALOR_MERCADO_JUGADOR_TEMP_ANTERIORES).extract()

                enlaces = response.xpath(self.XPATH_ENLACES_JUGADORES).extract()
                
                nombre_jugadores = response.xpath(self.XPATH_NOMBRE_JUGADORES).extract()

                foto_jugadores = response.xpath(self.XPATH_FOTO_JUGADORES).extract()

                pais = response.xpath(self.XPATH_PAIS).extract()

                fecha_nacimiento_edad = response.xpath(self.XPATH_FECHA_NACIMIENTO_EDAD).extract()

                posicion = response.xpath(self.XPATH_POSICION).extract()
                

                df_jugador = pd.DataFrame()

                df_jugador["id_jugador"] = [enlace.split("/")[-1] for enlace in enlaces]
                # df_jugador["url_foto_jugador"] = foto_jugadores
                df_jugador["nombre"] = [nombre_jugador.replace("\n","").strip() for nombre_jugador in nombre_jugadores if nombre_jugador.strip() != ""]
                df_jugador["url_jugador"] = enlaces
                df_jugador["nombre_tfmk"] = [enlace.split("/")[1] for enlace in enlaces]
                df_jugador["fecha_nacimiento"] = [fecha_nac.split(" ")[0] for fecha_nac in fecha_nacimiento_edad]
                df_jugador["edad"] = [int(edad.split(" ")[1].replace(",",".").replace("(","").replace(")","")) if edad.split(" ")[1].replace(",",".").replace("(","").replace(")","")!='-' else "" for edad in fecha_nacimiento_edad]
                # df_jugador["valor_mercado"] = valor_mercado #[float(vm.split(" ")[0].replace(",","")+"0000") if "mill." in vm else float(vm.split(" ")[0]+"000") for vm in valor_mercado] # revisar
                df_jugador["id_pais"] = [id_pais.split("/")[-1].split(".")[0] for id_pais in pais]
                df_jugador["altura"] = [altura.replace(",",".").replace("m","") for altura in alturas]
                df_jugador["posicion"] = [posicion_jugador.replace("\n","").strip() for posicion_jugador in posicion if posicion_jugador.strip() != ""] 

                lista_clubes_anteriores = []

                for club_ant in club_anterior:
                    id_club_anterior = club_ant.xpath(self.XPATH_CLUB_ANTERIOR_ID_RELATIVE).extract()

                    if id_club_anterior:
                        lista_clubes_anteriores.append(id_club_anterior[0].split("/")[-3])
                    else:
                        lista_clubes_anteriores.append("-")


                lista_fechas_fichaje = []
                for fecha_fich in fecha_fichaje:
                    fecha_fichaje_jugador = fecha_fich.xpath("./text()").extract()

                    if fecha_fichaje_jugador:
                        lista_fechas_fichaje.append(fecha_fichaje_jugador[0])
                    else:
                        lista_fechas_fichaje.append("")

                lista_pie = []
                for p in pie:
                    pie_jugador = p.xpath("./text()").extract()

                    if pie_jugador:
                        lista_pie.append(pie_jugador[0])
                    else:
                        lista_pie.append("")

                df_jugador["id_club_anterior"] = lista_clubes_anteriores # [club_ant.split("/")[-3] for club_ant in club_anterior] # revisar
                df_jugador["pie"] = lista_pie
                df_jugador["fecha_fichaje"] = lista_fechas_fichaje
                df_jugador["id_club"] = codigo_club
                df_jugador["id_temporada"] = temporada



                df_jugador["foto_jugador"] = df_jugador["id_jugador"]

                yield {'dataframe': df_jugador, 'tipo': 'jugador'}

            data = {'status': ['OK'], 'error': [''], 'traza': [''],'url': [response.url], 'metodo': metodo, 'insert_timestamp': datetime.now()}
            df_control = pd.DataFrame(data)
        except Exception as e:

            data = {'status': ['ERROR'], 'error': [str(e)], 'traza': [format_exc()], 'url': [response.url], 'metodo': metodo,'insert_timestamp': datetime.now()}
            df_control = pd.DataFrame(data)
        
        finally:

            yield {'dataframe': df_control, 'tipo': 'control'}

        pass

