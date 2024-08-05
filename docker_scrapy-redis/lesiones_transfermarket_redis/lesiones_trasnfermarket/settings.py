# Scrapy settings for lesiones_trasnfermarket project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html
import json

import os

BOT_NAME = "lesiones_trasnfermarket"

SPIDER_MODULES = ["lesiones_trasnfermarket.spiders"]
NEWSPIDER_MODULE = "lesiones_trasnfermarket.spiders"

LOG_LEVEL = 'DEBUG'  # Puedes ajustar el nivel según tus necesidades: 'CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'
LOG_ENABLED = True

# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Enables scheduling storing requests queue in redis.
SCHEDULER = "scrapy_redis.scheduler.Scheduler"

# Ensure all spiders share same duplicates filter through redis.
DUPEFILTER_CLASS = "scrapy_redis.dupefilter.RFPDupeFilter"

# Redis Connection URL
REDIS_URL = 'redis://redis:6379'

# Configura el nombre del grupo para los scrapers
GROUP_NAME = os.getenv('GROUP_NAME', 'main_spider_redis')

SCHEDULER_QUEUE_CLASS = 'scrapy_redis.queue.FifoQueue'

STATS_KEY = '%(spider)s:stats'
STATS_CLASS = 'scrapy_redis.stats.RedisStatsCollector'


SCHEDULER_PERSIST = True
# Ruta al archivo de configuración
CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), './config.json')

# Cargar configuración desde el archivo JSON
with open(CONFIG_FILE_PATH) as config_file:
    config = json.load(config_file)


MIN_SEASON = config.get('min_season', 2023)
MAX_SEASON = config.get('max_season', 2023)
CONTINENTS_TO_INCLUDE= config.get('continents_to_include', [])
COUNTRIES_TO_INCLUDE= config.get('countries_to_include', [])
LEAGUES_TO_INCLUDE= config.get('leagues_to_include', [])
CLUBS_TO_INCLUDE=config.get('clubs_to_include', [])
CURRENT_SEASON=config.get('current_season', 2024)
INCREMENTAL = config.get('incremental', True)
GET_PHOTOS = config.get('get_photos', False)

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
#DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
# SPIDER_MIDDLEWARES = {
#    "lesiones_trasnfermarket.middlewares.LesionesTrasnfermarketSpiderMiddleware": 543,
# }

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
# DOWNLOADER_MIDDLEWARES = {
#    "lesiones_trasnfermarket.middlewares.FixedUserAgentMiddleware": 543,
# }

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
#   "lesiones_trasnfermarket.pipelines.LesionesTrasnfermarketPipeline": 300,
ITEM_PIPELINES = {
       'lesiones_trasnfermarket.pipelines.PipelineClubs': 300
       
}
       # 'lesiones_trasnfermarket.pipelines.PipelineRedis': 200

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
AUTOTHROTTLE_TARGET_CONCURRENCY = 10
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"
