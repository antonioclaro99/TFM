import scrapy
import pandas as pd
import time
from scrapy_redis.spiders import RedisSpider
import json

from lesiones_trasnfermarket.items import RedisItem
from scrapy_redis.connection import get_redis_from_settings

from scrapy.utils.project import get_project_settings
import redis


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




class JugadoresSpider(RedisSpider):
    name = 'jugadores_spider_redis'
    redis_key = 'jugadores_spider_redis:items'
    redis_batch_size = 1

    # Max idle time(in seconds) before the spider stops checking redis and shuts down
    max_idle_time = 120

    engine = create_engine('mssql+pyodbc://scraper:scraper1234A@db-container:1433/tfm_lesiones?driver=ODBC+Driver+17+for+SQL+Server')
    Session = sessionmaker(bind=engine)
    session = Session()

    redis_conn = redis.StrictRedis(host='redis', port=6379, db=0)

    settings = get_project_settings()
    min_season = settings.get('MIN_SEASON', 2010)
    max_season = settings.get('MAX_SEASON', 2024)
    incremental = settings.get('INCREMENTAL', False)
    guardar_fotos = settings.get('GET_PHOTOS', False)


    seasons = [i for i in range(min_season,max_season+1)]
    
    BASE_URL = 'https://www.transfermarkt.es'
    

    # XPATH LESIONES
    XPATH_NUMERO_PAGS_LESIONES = '//*[@id="yw1"]/div[@class="pager"]/ul/li[@class="tm-pagination__list-item tm-pagination__list-item--icon-last-page"]/a/@href'


    XAPTH_CELDAS = '//*[@id="yw1"]/table/tbody/tr/td'

    XPATH_TEMPORADA_LESIONES = '//*[@id="yw1"]/table/tbody/tr/td[1]/text()'
    XPATH_LESION = '//*[@id="yw1"]/table/tbody/tr/td[2]/text()'
    XPATH_DESDE = '//*[@id="yw1"]/table/tbody/tr/td[3]/text()'
    XPATH_HASTA = '//*[@id="yw1"]/table/tbody/tr/td[4]'
    XPATH_DIAS = '//*[@id="yw1"]/table/tbody/tr/td[5]/text()'
    XPATH_PARTIDOS_PERDIDOS = '//*[@id="yw1"]/table/tbody/tr/td[6]//text()'

    XAPTH_FOTO_JUGADOR = '//*[@id="fotoauswahlOeffnen"]/img/@src'
    XPATH_DERECHOS_AUTOR_FOTO = '//*[@id="fotoauswahlOeffnen"]/div/span/text()'

    # XPATH ESTADISTICAS
    XAPTH_CELDAS = '//*[@class="grid-view"]/table/tbody/tr/td'

    XPATH_TEMPORADA = f'{XAPTH_CELDAS}[1]/text()'
    XPATH_ENLACE_COMPETICION = f'{XAPTH_CELDAS}[3]/a/@href'
    XPATH_NOMBRE_COMPETICION = f'{XAPTH_CELDAS}[3]//text()'
    XPATH_ENLACE_CLUB = f'{XAPTH_CELDAS}[4]/a/@href'
    XPATH_ONCE_INICIAL = f'{XAPTH_CELDAS}[5]//text()'
    XPATH_GOLES = f'{XAPTH_CELDAS}[6]/text()'
    XPATH_ASISTENCIAS = f'{XAPTH_CELDAS}[7]/text()'
    XPATH_GOLES_PP = f'{XAPTH_CELDAS}[8]/text()'
    XPATH_REVULSIVO = f'{XAPTH_CELDAS}[9]/text()'
    XPATH_SUSTITUIDO = f'{XAPTH_CELDAS}[10]/text()'
    XPATH_TARJETAS_AMARILLAS = f'{XAPTH_CELDAS}[11]/text()'
    XPATH_SEGUNDAS_TARJETAS_AMARILLAS = f'{XAPTH_CELDAS}[12]/text()'
    XPATH_TARJETAS_ROJAS= f'{XAPTH_CELDAS}[13]/text()'
    XPATH_PENALTIS_ANOTADOS = f'{XAPTH_CELDAS}[14]/text()'
    XPATH_MINUTOS_POR_GOL = f'{XAPTH_CELDAS}[15]/text()'
    XPATH_MINUTOS_JUGADOS = f'{XAPTH_CELDAS}[16]/text()'

    # PORTEROS
    # XPATH_POSICION_PAG_ESTADISTICAS = F'//*[@id="main"]/main/header/div[5]/div/ul[2]/li[2]/span/text()'
    XPATH_POSICION_PAG_ESTADISTICAS = F'//ul[@class="data-header__items"][2]/li[@class="data-header__label"][2]/span/text()'
    #//*[contains(text(), 'Posición:')]/span/text() #Probar con  este
    XPATH_GOLES_PP_PORTERO = f'{XAPTH_CELDAS}[7]/text()'
    XPATH_REVULSIVO_PORTERO = f'{XAPTH_CELDAS}[8]/text()'
    XPATH_SUSTITUIDO_PORTERO = f'{XAPTH_CELDAS}[9]/text()'
    XPATH_TARJETAS_AMARILLAS_PORTERO = f'{XAPTH_CELDAS}[10]/text()'
    XPATH_SEGUNDAS_TARJETAS_AMARILLAS_PORTERO = f'{XAPTH_CELDAS}[11]/text()'
    XPATH_TARJETAS_ROJAS_PORTERO = f'{XAPTH_CELDAS}[12]/text()'
    XPATH_GOLES_CONTRA_PORTERO = f'{XAPTH_CELDAS}[13]/text()'
    XPATH_PARTIDOS_IMBATIDO = f'{XAPTH_CELDAS}[14]/text()'
    XPATH_MINUTOS_JUGADOS_PORTERO = f'{XAPTH_CELDAS}[15]/text()'


    # XPATH PARTIDOS_SELECCIONES
    #https://www.transfermarkt.es/jesus-navas/nationalmannschaft/spieler/15956/verein_id/3375/plus/1
    #https://www.transfermarkt.es/umar-sadiq/nationalmannschaft/spieler/337715/

    #https://www.transfermarkt.es/jesus-navas/nationalmannschaft/spieler/15956/verein_id/3375/hauptwettbewerb//wettbewerb_id/nurEinsatz/0
    XPATH_CELDA_MINUTOS_JUGADOS_SELECCION_NACIONAL = '//*[@id="main"]/main/div[2]/div[1]/div[3]/div[3]/table/tbody[1]/tr/td[15]'
    XPATH_TEXTO_MINUTOS_JUGADOS_SELECCION_NACIONAL = f'{XPATH_CELDA_MINUTOS_JUGADOS_SELECCION_NACIONAL}/text()'
    XPATH_FECHA_PARTIDO_SELECCION_NACIONAL = f'{XPATH_CELDA_MINUTOS_JUGADOS_SELECCION_NACIONAL}/preceding-sibling::td[12]/text()'
    XPATH_POSICION_SELECCION_NACIONAL = f'{XPATH_CELDA_MINUTOS_JUGADOS_SELECCION_NACIONAL}/preceding-sibling::td[6]/a/@title'
    XPATH_ENLACE_PAIS_SELECCION = f'{XPATH_CELDA_MINUTOS_JUGADOS_SELECCION_NACIONAL}/preceding-sibling::td[10]/a/img/@src'
    XPATH_ENLACE_PAIS_SELECCION_CONTRA = f'{XPATH_CELDA_MINUTOS_JUGADOS_SELECCION_NACIONAL}/preceding-sibling::td[9]/a/img/@src'


    # XPATH ESTADISTICAS_GENERALES
    XPATH_ESTADISTICAS_COMPETICION ='//div[@class="row"]/div[1]/div[@class="box"]'

    XPATH_ESTADISTICAS_NOMBRE_COMPETICION = './div[1]/a/@name'


    XPATH_ESTADISTICAS_CELDA_MINUTOS_JUGADOS = "./div[3]//tr/td[{}]"

    XPATH_ESTADISTICAS_MINUTOS_JUGADOS = "./div[3]//tr/td[{}]/text()"

    XPATH_JORNADA_PARTIDO = XPATH_ESTADISTICAS_CELDA_MINUTOS_JUGADOS + '/preceding-sibling::td[{}]/a/text()'
    XPATH_FECHA_PARTIDO = XPATH_ESTADISTICAS_CELDA_MINUTOS_JUGADOS + '/preceding-sibling::td[{}]//text()'
    XPATH_LOCALIDAD = XPATH_ESTADISTICAS_CELDA_MINUTOS_JUGADOS + '/preceding-sibling::td[{}]//text()'
    XPATH_JUGADO_PARA= XPATH_ESTADISTICAS_CELDA_MINUTOS_JUGADOS + '/preceding-sibling::td[{}]/a/img/@src'
    XPATH_JUGADO_CONTRA = XPATH_ESTADISTICAS_CELDA_MINUTOS_JUGADOS + '/preceding-sibling::td[{}]/a/img/@src'
    XPATH_RESULTADO = XPATH_ESTADISTICAS_CELDA_MINUTOS_JUGADOS + '/preceding-sibling::td[7]/a/span/text()'
    XPATH_POSICION_PARTIDO = XPATH_ESTADISTICAS_CELDA_MINUTOS_JUGADOS + '/preceding-sibling::td[6]/a/text()'

    hostname = os.getenv('HOSTNAME')

    # Definir la tabla de imágenes usando SQLAlchemy ORM
    metadata = MetaData()
    imagenes_jugador_table = Table('imagenes_jugador', metadata,
        Column('id_jugador', String(255)),
        Column('ImageData', LargeBinary),
        Column('copyright_imagen', String(255))
    )

    # Crear la tabla si no existe
    metadata.create_all(engine)

    # Función para descargar una imagen
    def download_image(self,image_url):
        try:
            response = requests.get(image_url)
            if response.status_code == 200:
                return response.content
            else:
                return None
        except:
            return None

    # Función para guardar la imagen en la base de datos
    def save_image_to_db(self,id_jugador, image_data,derechos_autor_imagen_jugador):
        ins = self.imagenes_jugador_table.insert().values(id_jugador=id_jugador, ImageData=image_data,copyright_imagen=derechos_autor_imagen_jugador)
        self.session.execute(ins)
        self.session.commit()

    def scrapear_lesiones(self,response):
        codigo_jugador = response.url.split("/")[6]

        temporada = response.xpath(self.XPATH_TEMPORADA_LESIONES).extract()
        
        lesion = response.xpath(self.XPATH_LESION).extract()

        fecha_desde = response.xpath(self.XPATH_DESDE).extract()

        fecha_hasta = response.xpath(self.XPATH_HASTA)

        dias = response.xpath(self.XPATH_DIAS).extract()

        partidos_perdidos = response.xpath(self.XPATH_PARTIDOS_PERDIDOS).extract()

        df_lesiones = pd.DataFrame()

        df_lesiones["temporada"] = ["20"+temp.split("/")[0] for temp in temporada] 
        df_lesiones["lesion"] = lesion
        df_lesiones["fecha_desde"] = fecha_desde

        lista_fecha_hasta = []
        for h in fecha_hasta:
            hasta = h.xpath("./text()").extract()

            if hasta:
                lista_fecha_hasta.append(hasta[0])
            else:
                lista_fecha_hasta.append("")

        df_lesiones["fecha_hasta"] = lista_fecha_hasta
        df_lesiones["dias"] = [int(num_dias.split(" ")[0]) for num_dias in dias]
        df_lesiones["partidos_perdidos"] = [int(num_partidos_perdidos.replace("-","0").replace("?","0")) for num_partidos_perdidos in partidos_perdidos] # revisar HAY INTYERROGACIONES EN ALGUNOS
        df_lesiones["id_jugador"] = codigo_jugador

        return df_lesiones

        

    def __init__(self, *args, **kwargs):
        super(JugadoresSpider, self).__init__(*args, **kwargs)
        print(f"{self.hostname}")
        logging.debug(f"Instancia {self.hostname} inicializada.")
        
        while self.redis_conn.exists('main_spider_redis:lock'):
            logging.debug(f"{self.hostname} bloqueado")
            time.sleep(1)

        self.instance_type = os.getenv('INSTANCE_TYPE', 'regular')


        if self.instance_type=="rw":

            seasons_query = ', '.join(map(str, self.seasons))            
            
            query_id_jugadores = f"""SELECT DISTINCT id_jugador FROM tfm_lesiones.dbo.jugador where id_temporada in ({seasons_query}) and id_club in (SELECT distinct id_club_tfmk from tfm_lesiones.dbo.clubs_to_scrape)"""
            
            delete_query_lesiones = f"delete from dbo.lesiones where id_jugador in ({query_id_jugadores})"
            delete_query_estadisticas = f"delete from dbo.estadisticas_generales where temporada in  ({seasons_query}) and id_jugador in ({query_id_jugadores})"

            query_delete_1 = f"""IF (EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND  TABLE_NAME = 'lesiones'))
                                BEGIN
                                {delete_query_lesiones}
                                END"""
            
            query_delete_2 = f"""IF (EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND  TABLE_NAME = 'estadisticas_generales'))
                                BEGIN
                                {delete_query_estadisticas}
                                END"""

            self.session.execute(text(query_delete_1))

            self.session.execute(text(query_delete_2))

            self.session.commit()

            query = f"""SELECT DISTINCT url_jugador FROM tfm_lesiones.dbo.jugador where id_temporada in ({seasons_query}) and id_club in (SELECT distinct id_club_tfmk from tfm_lesiones.dbo.clubs_to_scrape);"""

            
            df = pd.read_sql(query,self.engine)

            for url_jugador in df['url_jugador']:
                    
                    url_lesiones = f"{self.BASE_URL}{url_jugador}/plus/1".replace("/profil/","/verletzungen/")

                    item = {"url": url_lesiones, "meta": {"metodo": "parse_lesiones_primera_pag"}}
                    item_json = json.dumps(item)
                    self.redis_conn.lpush(self.redis_key, item_json)

                    for temporada in self.seasons:
                        url_estadisticas_general =f"{self.BASE_URL}{url_jugador}/saison/{str(temporada)}/verein/0/liga/0/wettbewerb//pos/0/trainer_id/0".replace("/profil/","/leistungsdatendetails/")                 

                        item = {"url": url_estadisticas_general, "meta": {"metodo": "parse_estadisticas_general"}}
                        item_json = json.dumps(item)
                        self.redis_conn.lpush(self.redis_key, item_json)
        
    def parse(self, response):

        metodo = response.meta.get('metodo')

        try:              

            if "parse_lesiones" in metodo:

                if metodo == "parse_lesiones_primera_pag":
                    numero_pags_lesiones = response.xpath(self.XPATH_NUMERO_PAGS_LESIONES).extract()

                    if numero_pags_lesiones:
                        numero_pags_lesiones = int(numero_pags_lesiones[0].split("/")[-1])
                        for i in range(2,numero_pags_lesiones+1):
                            url_pags_lesiones = f"{response.url}/page/{i}"
                            item = {"url": url_pags_lesiones, "meta": {"metodo": "parse_lesiones_otras_paginas"}}
                            item_json = json.dumps(item)
                            self.redis_conn.lpush(self.redis_key, item_json)
                    
                    if self.guardar_fotos:
                        enlace_foto_jugador = response.xpath(self.XAPTH_FOTO_JUGADOR).extract()

                        derechos_autor_imagen_jugador = response.xpath(self.XPATH_DERECHOS_AUTOR_FOTO).extract()

                        id_jugador = enlace_foto_jugador.split("/")[-1].split("-")[0]

                        if "/default" not in enlace_foto_jugador and "." not in str(id_jugador):

                            image_data = self.download_image(enlace_foto_jugador)
                            if image_data:
                                if not derechos_autor_imagen_jugador:
                                    derechos_autor_imagen_jugador = ""
                                self.save_image_to_db(codigo_jugador, image_data,derechos_autor_imagen_jugador)
                                print(f"Imagen {codigo_jugador} guardada en la base de datos.")
                            else:
                                print(f"Error al descargar la imagen {codigo_jugador}.")

                codigo_jugador = response.url.split("/")[6]

                temporada = response.xpath(self.XPATH_TEMPORADA_LESIONES).extract()
                
                lesion = response.xpath(self.XPATH_LESION).extract()

                fecha_desde = response.xpath(self.XPATH_DESDE).extract()

                fecha_hasta = response.xpath(self.XPATH_HASTA)

                dias = response.xpath(self.XPATH_DIAS).extract()

                partidos_perdidos = response.xpath(self.XPATH_PARTIDOS_PERDIDOS).extract()

                df_lesiones = pd.DataFrame()

                df_lesiones["temporada"] = ["20"+temp.split("/")[0] for temp in temporada] 
                df_lesiones["lesion"] = lesion
                df_lesiones["fecha_desde"] = fecha_desde

                lista_fecha_hasta = []
                for h in fecha_hasta:
                    hasta = h.xpath("./text()").extract()

                    if hasta:
                        lista_fecha_hasta.append(hasta[0])
                    else:
                        lista_fecha_hasta.append("")

                df_lesiones["fecha_hasta"] = lista_fecha_hasta
                df_lesiones["dias"] = [int(num_dias.split(" ")[0]) for num_dias in dias]
                df_lesiones["partidos_perdidos"] = [int(num_partidos_perdidos.replace("-","0").replace("?","0")) for num_partidos_perdidos in partidos_perdidos] # revisar HAY INTYERROGACIONES EN ALGUNOS
                df_lesiones["id_jugador"] = codigo_jugador

                yield {'dataframe': df_lesiones, 'tipo': 'lesiones'}
   
            elif metodo == "parse_estadisticas_general":

                codigo_jugador = response.url.split("/")[6]

                codigo_temporada = response.url.split("/")[8]

                competiciones = response.xpath(self.XPATH_ESTADISTICAS_COMPETICION)

                for competicion in competiciones[1:]:
                    nombre_competicion = competicion.xpath(self.XPATH_ESTADISTICAS_NOMBRE_COMPETICION).extract()[0]

                    intentos = 3
                    maximo = 15
                    encontrado = False
                    while not encontrado and intentos>0:

                        longitud = len(competicion.xpath(self.XPATH_ESTADISTICAS_CELDA_MINUTOS_JUGADOS.format(maximo)))

                        if longitud>0:
                            
                            encontrado = True
                            break
                        else:
                            maximo = maximo -1
                            intentos = intentos-1

                    if encontrado:

                        minutos_jugados = competicion.xpath(self.XPATH_ESTADISTICAS_MINUTOS_JUGADOS.format(maximo)).extract()

                        jornada = competicion.xpath(self.XPATH_JORNADA_PARTIDO.format(maximo,maximo-1)).extract()
                        fecha_partido = competicion.xpath(self.XPATH_FECHA_PARTIDO.format(maximo,maximo-2)).extract()
                        localidad = competicion.xpath(self.XPATH_LOCALIDAD.format(maximo,maximo-3)).extract()
                        jugado_para = competicion.xpath(self.XPATH_JUGADO_PARA.format(maximo,maximo-4)).extract()
                        if maximo==15 :
                            jugado_contra = competicion.xpath(self.XPATH_JUGADO_CONTRA.format(maximo,maximo-6)).extract()
                        elif maximo==14 or maximo==13:
                            jugado_contra = competicion.xpath(self.XPATH_JUGADO_CONTRA.format(maximo,maximo-5)).extract()
                            
                        resultado = competicion.xpath(self.XPATH_RESULTADO.format(maximo)).extract()
                        posicion = competicion.xpath(self.XPATH_POSICION_PARTIDO.format(maximo)).extract()
                
                        df_estadisticas = pd.DataFrame()

                        if minutos_jugados:

                            df_estadisticas["minutos_jugados"] = [int(min_jugados.replace("'","")) for min_jugados in minutos_jugados]
                            df_estadisticas["id_jugador"] = codigo_jugador
                            df_estadisticas["nombre_competicion"] = nombre_competicion

                            df_estadisticas["jornada"] = jornada
                            df_estadisticas["fecha_partido"] = fecha_partido
                            df_estadisticas["localidad"] = localidad
                            df_estadisticas["jugado_para"] = [id_equipo.split('/')[-1].split('.')[0].split("_")[0] for id_equipo in jugado_para]
                            df_estadisticas["jugado_contra"] = [id_equipo.split('/')[-1].split('.')[0].split("_")[0] for id_equipo in jugado_contra]
                            df_estadisticas["resultado"] = resultado
                            # df_estadisticas["posicion"] = posicion
                            df_estadisticas["temporada"] = codigo_temporada

                            df_estadisticas = df_estadisticas.replace("-", "0")
                            primer_columna = df_estadisticas.columns[0]
                            column_order = list(df_estadisticas.columns[1:]) + [primer_columna]
                            df_estadisticas = df_estadisticas[column_order]

                            yield {'dataframe': df_estadisticas, 'tipo': 'estadisticas_generales'}

            data = {'status': ['OK'], 'error': [''], 'traza': [''],'url': [response.url], 'metodo': metodo}
            df_control = pd.DataFrame(data)
        except Exception as e:

            data = {'status': ['ERROR'], 'error': [str(e)], 'traza': [format_exc()], 'url': [response.url], 'metodo': metodo}
            df_control = pd.DataFrame(data)
        
        finally:

            yield {'dataframe': df_control, 'tipo': 'control'}

        pass

