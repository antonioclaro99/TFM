import scrapy
import pandas as pd
import time

class MainSpider(scrapy.Spider):
    name = 'main_spider'

    codigos_ligas = [('laliga','ES1'),('premier-league','GB1'),('ligue-1','FR1'),('bundesliga','L1'),('serie-a','IT1')]

    seasons = [i for i in range(2014,2024)]

    start_urls = []

    for season in seasons:
        for codigo_liga in codigos_ligas:
            url = f'https://www.transfermarkt.es/{codigo_liga[0]}/startseite/wettbewerb/{codigo_liga[1]}/plus/?saison_id={season}'

            start_urls.append(url)


    # start_urls = ['https://www.transfermarkt.es/laliga/startseite/wettbewerb/ES1',
    #               'https://www.transfermarkt.es/premier-league/startseite/wettbewerb/GB1',
    #               'https://www.transfermarkt.es/ligue-1/startseite/wettbewerb/FR1',
    #               'https://www.transfermarkt.es/bundesliga/startseite/wettbewerb/L1',
    #               'https://www.transfermarkt.es/serie-a/startseite/wettbewerb/IT1']
    
    BASE_URL = 'https://www.transfermarkt.es'

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
    XPATH_NOMBRE_JUGADORES = '//*[@id="yw1"]/table/tbody/tr/td[2]/table/tbody/tr[1]/td[2]/a/text()'
    XPATH_NOMBRE_JUGADORES = '//table[@class="inline-table"]//tr[1]/td[2]/a/text()'
    XPATH_FECHA_NACIMIENTO_EDAD = '//*[@id="yw1"]/table/tbody/tr/td[3]/text()'
    XPATH_PAIS = '//*[@id="yw1"]/table/tbody/tr/td[4]/img[1]/@title'
    XPATH_ALTURA = '//*[@id="yw1"]/table/tbody/tr/td[5]/text()'
    XPATH_ENLACES_BANDERAS = '//*[@id="yw1"]/table/tbody//td[4]/img[1]/@src'
    XPATH_PIE = '//*[@id="yw1"]/table/tbody/tr/td[6]/text()'
    XPATH_FECHA_FICHAJE = '//*[@id="yw1"]/table/tbody/tr/td[7]/text()'
    XPATH_CLUB_ANTERIOR_ID = '//*[@id="yw1"]/table/tbody/tr/td[8]/a/@href'
    XPATH_VALOR_MERCADO_JUGADOR = '//*[@id="yw1"]/table/tbody/tr/td[10]/a/text()'
    XPATH_POSICION = '//table[@class="inline-table"]//tr[2]/td[1]/text()'
    

     # XPATH LESIONES
    XAPTH_CELDAS = '//*[@id="yw1"]/table/tbody/tr/td'

    XPATH_TEMPORADA = '//*[@id="yw1"]/table/tbody/tr/td[1]/text()'
    XPATH_LESION = '//*[@id="yw1"]/table/tbody/tr/td[2]/text()'
    XPATH_DESDE = '//*[@id="yw1"]/table/tbody/tr/td[3]/text()'
    XPATH_HASTA = '//*[@id="yw1"]/table/tbody/tr/td[4]/text()'
    XPATH_DIAS = '//*[@id="yw1"]/table/tbody/tr/td[5]/text()'
    XPATH_PARTIDOS_PERDIDOS = '//*[@id="yw1"]/table/tbody/tr/td[6]//text()'

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



    def start_requests(self):
        # Configura el user-agent en las solicitudes iniciales
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'}
        for url in self.start_urls:
            yield scrapy.Request(url, headers=headers, callback=self.parse)


    def parse(self, response):

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
        df_clubs["escudo_club"] = enlaces_escudos
        df_clubs["edad_media_club"] = [float(edad.replace(",",".")) for edad in edad_media]
        df_clubs["valor_mercado_millones_euros_club"] = [float(vm.split(" ")[0].replace(",","000.")) if "mil mill." in vm else float(vm.split(" ")[0].replace(",",".")) for vm in valor_mercado]
        df_clubs["id_liga_tfmk"] = codigo_liga
        df_clubs["temporada"] = temporada

        yield {'dataframe': df_clubs, 'tipo': 'clubs'}

        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'}
        # for url_club in df_clubs["url_tfmk"]:

        #     # https://www.transfermarkt.es/real-madrid-cf/kader/verein/418/saison_id/2023/plus/1

        #     # https://www.transfermarkt.es/fc-brentford/startseite/verein/1148/saison_id/2023/plus/1
        #     url = f"{self.BASE_URL}{url_club}/plus/1".replace("/startseite/","/kader/")
        #     yield scrapy.Request(url, headers=headers, callback=self.parse_jugadores)


    def parse_jugadores(self, response):

        codigo_club = response.url.split("/")[6]

        enlaces = response.xpath(self.XPATH_ENLACES_JUGADORES).extract()
        
        nombre_jugadores = response.xpath(self.XPATH_NOMBRE_JUGADORES).extract()

        pais = response.xpath(self.XPATH_PAIS).extract()

        alturas = response.xpath(self.XPATH_ALTURA).extract()

        pie = response.xpath(self.XPATH_PIE).extract()

        fecha_fichaje = response.xpath(self.XPATH_FECHA_FICHAJE).extract()

        club_anterior = response.xpath(self.XPATH_CLUB_ANTERIOR_ID).extract()

        enlaces_banderas = response.xpath(self.XPATH_ENLACES_BANDERAS).extract()

        fecha_nacimiento_edad = response.xpath(self.XPATH_FECHA_NACIMIENTO_EDAD).extract()

        posicion = response.xpath(self.XPATH_POSICION).extract()
        
        valor_mercado = response.xpath(self.XPATH_VALOR_MERCADO_JUGADOR).extract()

        df_jugador = pd.DataFrame()

        df_jugador["id_tfmk"] = [enlace.split("/")[-1] for enlace in enlaces]
        df_jugador["nombre"] = [nombre_jugador.replace("\n","").strip() for nombre_jugador in nombre_jugadores if nombre_jugador.strip() != ""]
        df_jugador["url_tfmk"] = enlaces
        df_jugador["nombre_tfmk"] = [enlace.split("/")[1] for enlace in enlaces]
        df_jugador["bandera"] = enlaces_banderas
        df_jugador["fecha_nacimiento"] = [fecha_nac.split(" ")[0] for fecha_nac in fecha_nacimiento_edad]
        df_jugador["edad"] = [int(edad.split(" ")[1].replace(",",".").replace("(","").replace(")","")) for edad in fecha_nacimiento_edad]
        #df_jugador["valor_mercado"] = [float(vm.split(" ")[0].replace(",","")+"0000") if "mill." in vm else float(vm.split(" ")[0]+"000") for vm in valor_mercado] # revisar
        df_jugador["pais"] = pais
        df_jugador["altura"] = [altura.replace(",",".").replace("m","") for altura in alturas]
        df_jugador["posicion"] = [posicion_jugador.replace("\n","").strip() for posicion_jugador in posicion if posicion_jugador.strip() != ""] 
        #df_jugador["id_club_anterior"] = [club_ant.split("/")[-3] for club_ant in club_anterior] # revisar
        df_jugador["pie"] = pie
        df_jugador["fecha_fichaje"] = fecha_fichaje
        df_jugador["id_equipo"] = codigo_club


        yield {'dataframe': df_jugador, 'tipo': 'jugador'}


        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'}
        for url_jugador in df_jugador["url_tfmk"]:
            url_lesiones = f"{self.BASE_URL}{url_jugador}/plus/1".replace("/profil/","/verletzungen/")
            url_estadisticas = f"{self.BASE_URL}{url_jugador}/plus/1".replace("/profil/","/detaillierteleistungsdaten/")
            yield scrapy.Request(url_lesiones, headers=headers, callback=self.parse_lesiones)
            yield scrapy.Request(url_estadisticas, headers=headers, callback=self.parse_estadisticas)

        # https://www.transfermarkt.es/thibaut-courtois/verletzungen/spieler/108390/plus/1
        # /mykhaylo-mudryk/profil/spieler/537860
            

        #https://www.transfermarkt.es/jesus-navas/detaillierteleistungsdaten/spieler/15956/plus/1
            

    def parse_lesiones(self, response):
        codigo_jugador = response.url.split("/")[6]



        temporada = response.xpath(self.XPATH_TEMPORADA).extract()
        
        lesion = response.xpath(self.XPATH_LESION).extract()

        fecha_desde = response.xpath(self.XPATH_DESDE).extract()

        fecha_hasta = response.xpath(self.XPATH_HASTA).extract()

        dias = response.xpath(self.XPATH_DIAS).extract()

        partidos_perdidos = response.xpath(self.XPATH_PARTIDOS_PERDIDOS).extract()

        df_lesiones = pd.DataFrame()

        df_lesiones["temporada"] = temporada
        df_lesiones["lesion"] = lesion
        df_lesiones["fecha_desde"] = fecha_desde
        df_lesiones["fecha_hasta"] = fecha_hasta
        df_lesiones["dias"] = [int(num_dias.split(" ")[0]) for num_dias in dias]
        df_lesiones["partidos_perdidos"] = [int(num_partidos_perdidos.replace("-","0").replace("?","0")) for num_partidos_perdidos in partidos_perdidos] # revisar HAY INTYERROGACIONES EN ALGUNOS
        df_lesiones["id_jugador"] = codigo_jugador

        yield {'dataframe': df_lesiones, 'tipo': 'lesiones'}


    def parse_estadisticas(self, response):

        codigo_jugador = response.url.split("/")[6]

        posicion = response.xpath(self.XPATH_POSICION_PAG_ESTADISTICAS).extract()[0]        

        temporada = response.xpath(self.XPATH_TEMPORADA).extract()
        enlace_competicion = response.xpath(self.XPATH_ENLACE_COMPETICION).extract()
        nombre_competicion = response.xpath(self.XPATH_NOMBRE_COMPETICION).extract()
        enlace_club = response.xpath(self.XPATH_ENLACE_CLUB).extract()
        once_inicial = response.xpath(self.XPATH_ONCE_INICIAL).extract()
        goles = response.xpath(self.XPATH_GOLES).extract()
        asistencias = response.xpath(self.XPATH_ASISTENCIAS).extract()
        goles_pp = response.xpath(self.XPATH_GOLES_PP).extract()
        revulsivo = response.xpath(self.XPATH_REVULSIVO).extract()
        sustituido = response.xpath(self.XPATH_SUSTITUIDO).extract()
        tarjetas_amarillas = response.xpath(self.XPATH_TARJETAS_AMARILLAS).extract()
        segundas_tarjetas_amarillas = response.xpath(self.XPATH_SEGUNDAS_TARJETAS_AMARILLAS).extract()
        tarjetas_rojas = response.xpath(self.XPATH_TARJETAS_ROJAS).extract()
        penaltis_anotados = response.xpath(self.XPATH_PENALTIS_ANOTADOS).extract()
        minutos_por_gol = response.xpath(self.XPATH_MINUTOS_POR_GOL).extract()
        minutos_jugados = response.xpath(self.XPATH_MINUTOS_JUGADOS).extract()

        goles_pp_portero = response.xpath(self.XPATH_GOLES_PP_PORTERO).extract()
        revulsivo_portero = response.xpath(self.XPATH_REVULSIVO_PORTERO).extract()
        sustituido_portero = response.xpath(self.XPATH_SUSTITUIDO_PORTERO).extract()
        tarjetas_amarillas_portero = response.xpath(self.XPATH_TARJETAS_AMARILLAS_PORTERO).extract()
        segundas_tarjetas_amarillas_portero = response.xpath(self.XPATH_SEGUNDAS_TARJETAS_AMARILLAS_PORTERO).extract()
        tarjetas_rojas_portero = response.xpath(self.XPATH_TARJETAS_ROJAS_PORTERO).extract()
        goles_contra_portero = response.xpath(self.XPATH_GOLES_CONTRA_PORTERO).extract()
        partidos_imbatido_portero = response.xpath(self.XPATH_PARTIDOS_IMBATIDO).extract()
        minutos_jugados_portero = response.xpath(self.XPATH_MINUTOS_JUGADOS_PORTERO).extract()


        df_estadisticas = pd.DataFrame()

        df_estadisticas["temporada"] = temporada
        df_estadisticas["id_competicion"] = [url_comp.split("/")[-3] for url_comp in enlace_competicion]
        df_estadisticas["nombre_competicion"] = nombre_competicion
        df_estadisticas["id_club"] = [url_club.split("/")[-3] for url_club in enlace_club]
        df_estadisticas["once_inicial"] = once_inicial
        df_estadisticas["goles"] = goles

        columna_nula = ["-"] * len(df_estadisticas["temporada"])
        if posicion.upper().strip()=='PORTERO':
            df_estadisticas["asistencias"] = columna_nula
            df_estadisticas["goles_pp"] = goles_pp_portero
            df_estadisticas["revulsivo"] = revulsivo_portero
            df_estadisticas["sustituido"] = sustituido_portero
            df_estadisticas["tarjetas_amarillas"] = tarjetas_amarillas_portero
            df_estadisticas["segundas_tarjetas_amarillas"] = segundas_tarjetas_amarillas_portero
            df_estadisticas["tarjetas_rojas"] = tarjetas_rojas_portero
            df_estadisticas["penaltis_anotados"] = columna_nula
            df_estadisticas["goles_contra_portero"] = goles_contra_portero
            df_estadisticas["partidos_imbatido_portero"] = partidos_imbatido_portero
            df_estadisticas["minutos_por_gol"] = columna_nula
            df_estadisticas["minutos_jugados"] = [min_jugados.replace("'","") for min_jugados in minutos_jugados_portero]

        else:
            df_estadisticas["asistencias"] = asistencias
            df_estadisticas["goles_pp"] = goles_pp
            df_estadisticas["revulsivo"] = revulsivo
            df_estadisticas["sustituido"] = sustituido
            df_estadisticas["tarjetas_amarillas"] = tarjetas_amarillas
            df_estadisticas["segundas_tarjetas_amarillas"] = segundas_tarjetas_amarillas
            df_estadisticas["tarjetas_rojas"] = tarjetas_rojas
            df_estadisticas["penaltis_anotados"] = penaltis_anotados
            df_estadisticas["goles_contra_portero"] = columna_nula
            df_estadisticas["partidos_imbatido_portero"] = columna_nula
            df_estadisticas["minutos_por_gol"] = [min_gol.replace("'","") for min_gol in minutos_por_gol]
            df_estadisticas["minutos_jugados"] = [min_jugados.replace("'","") for min_jugados in minutos_jugados]
        
        df_estadisticas["id_jugador"] = codigo_jugador

        df_estadisticas = df_estadisticas.replace("-", "0")

        yield {'dataframe': df_estadisticas, 'tipo': 'estadisticas'}