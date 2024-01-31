import scrapy
import pandas as pd

class Lesiones(scrapy.Spider):
    name = 'jugadores'
    start_urls = ['https://www.transfermarkt.es/real-madrid/startseite/verein/418/saison_id/2023']
    start_urls = ['https://fbref.com/en/players/686cbe7d/matchlogs/2023-2024/Umar-Sadiq-Match-Logs']
    

    def start_requests(self):
        # Configura el user-agent en las solicitudes iniciales
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'}
        for url in self.start_urls:
            yield scrapy.Request(url, headers=headers, callback=self.parse)


    def parse(self, response):
        # Aquí defines cómo extraer los datos de la página
        # Puedes usar selectores XPath o CSS para seleccionar elementos HTML

        html_content = response.body

        # Utilizar Pandas y XPath para leer la tabla
        tablas = pd.read_html(html_content, flavor='lxml')

        print("hola" + tablas)

        # Supongamos que la tabla que te interesa es la primera (índice 0)
        if tablas:
            tabla = tablas
            print("hola" + tabla)
        else:
            self.log("No se encontraron tablas en la página.")