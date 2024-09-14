# Proyecto de Análisis de Datos con Docker, Python y Power BI

Este proyecto incluye varios componentes que permiten la extracción de datos de Transfermarkt y el posterior análisis de datos utilizando contenedores Docker, notebooks de Python y Power BI. A continuación, se describen los pasos para ejecutar cada parte del proyecto.

## Requisitos previos

Asegúrate de tener instalados los siguientes componentes en tu máquina:

- [Docker](https://www.docker.com/get-started)
- [Docker Compose](https://docs.docker.com/compose/install/)
- [Python 3.x](https://www.python.org/downloads/)
- [Power BI Desktop](https://powerbi.microsoft.com/desktop/)

## Estructura del repositorio

La estructura básica del repositorio es la siguiente:
```bash
├── bi/                         # Carpeta para archivos de Power BI y librerías específicas para la ejecución del informe
├── data/                       # Datos reducidos del proyecto
├── docker_db_redis/            # Configuración de Docker para Redis
├── docker_scrapy-redis/        # Configuración de Docker para Scrapy con Redis
├── llm/                        # Notebook de clasificación de lesiones con LLM
├── regresion/                  # Notebook para análisis de regresión
├── requirements.txt            # Librerías necesarias para los notebooks de Python
├── .gitignore
├── .gitattributes
```

## Instalación de librerías

### 1. Instalación de librerías para Python

El archivo `requirements.txt` ubicado en la raíz del repositorio incluye las librerías necesarias para los notebooks de Python.

Para instalar estas dependencias, ejecuta:
```bash
pip install -r requirements.txt
```
### 2. Instalación de librerías para Power BI

El archivo `bi/requirements.txt` incluye las dependencias necesarias para ejecutar un visual realizado con Python en PowerBi.

Para instalar estas dependencias, ve a la carpeta `bi` y ejecuta:
```bash
cd bi/
pip install -r requirements.txt
````
## Ejecución de Contenedores Docker

### 1. Crear red para los contenedores
Crea la red para que los contenedores puedan comunicarse entre sí.

```bash
docker network create my_shared_network_tfm
```
### 1. Docker Compose

Para gestionar los contenedores de Redis y Scrapy con Redis, puedes utilizar Docker Compose. Los archivos de configuración para estos contenedores se encuentran en las carpetas `docker_db_redis` y `docker_scrapy-redis`.

Para ejecutar todos los contenedores definidos en `docker_db_redis`:
```bash
cd docker_db_redis
docker-compose up --build
```

Para ejecutar los contenedores del scraper main definidos en `docker_scrapy-redis`:
```bash
cd docker_scrapy-redis
docker compose -f docker-compose.main.yml up --build --scale crawler=3
```
Podemos indicar el número de réplicas que queremos del contenedor crawler, en este caso 3.

Para ejecutar los contenedores del scraper jugadores definidos en `docker_scrapy-redis`:
```bash
cd docker_scrapy-redis
docker compose -f docker-compose.jugadores.yml up --build --scale crawler_jugadores=4
```
Podemos indicar el número de réplicas que queremos del contenedor crawler_jugadores, en este caso 4.

Para detener los contenedores, puedes ejecutar el siguiente comando, cambiando el nombre del docker-compose a ejecutar y entrando a la carpeta donde se encuentre el docker-compose:
```bash
cd carpeta
docker compose -f nombre-docker-compose.yml down
```

## Ejecución de Notebooks de Python

Los notebooks de Python están organizados en las carpetas `llm` y `regresion`. Puedes ejecutar los notebooks utilizando Jupyter Notebook o JupyterLab.

Si no tienes Jupyter instalado, puedes instalarlo con el siguiente comando:
```bash
pip install jupyterlab
```
Para iniciar Jupyter, navega a la carpeta raíz del proyecto y ejecuta:
```bash
jupyter lab
```
Esto abrirá el entorno de JupyterLab en tu navegador, donde podrás explorar y ejecutar los notebooks disponibles.

## Consideraciones adicionales

- Asegúrate de que los contenedores de base de datos están corriendo para la ejecución de los contenedores scrapers o de los notebooks.
- Configura PowerBi para que tome Python en el entorno donde has instalado las librerías.
- Los contenedores scrapers se detienen automáticamente cuando terminan de ejecutarse.
- Se puede incluir el comando '-d' al iniciar los contenedores para no verlos en la terminal.
  
## Contribuciones

Si deseas contribuir al proyecto, por favor, abre un pull request o envía un issue describiendo el problema o mejora que deseas implementar.

## Licencia

Este proyecto está licenciado bajo [MIT License](LICENSE).
