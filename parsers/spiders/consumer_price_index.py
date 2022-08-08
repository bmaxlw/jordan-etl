# Date: 08/08/22
# Frequency: Monthly

import scrapy
from sqlalchemy.orm import Session
from database import database as DB
from database import connector as CN
from scrapy.crawler import CrawlerProcess

class ConsumerPriceSpider(scrapy.Spider):
    name = "consumer-price-index"
    start_urls = ['https://tradingeconomics.com/jordan/consumer-price-index-cpi',]
    engine = CN.Connector().connect() # modifiable, default args used

    def parse(self, response):
        response_list = response.css('td::text').getall()
        frequency = str(response_list[48]).strip()
        start_date = str(response_list[5]).strip()
        measure_units = str(response_list[47]).strip()
        value = str(response_list[42]).strip()

        with Session(self.engine) as session:
            consumer_price_index_instance = DB.Indicator(
                indicator_name = 'Consumer Price Index',
                category = 'Prices',
                frequency = frequency,
                start_date = start_date,
                measure_units = measure_units,
                value = value
            )
            session.add(consumer_price_index_instance)
            session.commit()

if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(ConsumerPriceSpider)
    process.start()