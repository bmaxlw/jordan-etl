# Date: 08/08/22
# Frequency: Monthly

import scrapy
from sqlalchemy.orm import Session
from database import database as DB
from database import connector as CN
from scrapy.crawler import CrawlerProcess

class InflationRateSpider(scrapy.Spider):
    name = "inflation-rate"
    start_urls = ['https://tradingeconomics.com/jordan/inflation-cpi',]
    engine = CN.Connector().connect() # modifiable, default args used

    def parse(self, response):
        response_list = response.css('td::text').getall()
        frequency = str(response_list[57]).strip()
        start_date = str(response_list[32]).strip()
        measure_units = str(response_list[56]).strip()
        value = str(response_list[51]).strip()

        with Session(self.engine) as session:
            inflation_rate_instance = DB.Indicator(
                indicator_name = 'Inflation Rate',
                category = 'Prices',
                frequency = frequency,
                start_date = start_date,
                measure_units = measure_units,
                value = value
            )
            session.add(inflation_rate_instance)
            session.commit()

if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(InflationRateSpider)
    process.start()
