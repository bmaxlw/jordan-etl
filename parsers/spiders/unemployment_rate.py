# Date: 08/08/22
# Frequency: Quarterly

import scrapy
from sqlalchemy.orm import Session
from database import database as DB
from database import connector as CN
from scrapy.crawler import CrawlerProcess

class UnemploymentRateSpider(scrapy.Spider):
    name = "unemployment-rate"
    start_urls = ['https://tradingeconomics.com/jordan/unemployment-rate',]
    engine = CN.Connector().connect() # modifiable, default args used

    def parse(self, response):
        response_list = response.css('td::text').getall()
        frequency = str(response_list[63]).strip()
        start_date = str(response_list[56]).strip()
        measure_units = str(response_list[62]).strip()
        value = str(response_list[57]).strip()

        with Session(self.engine) as session:
            unemployment_rate_instance = DB.Indicator(
                indicator_name = 'Unemployment Rate',
                category = 'Labour',
                frequency = frequency,
                start_date = start_date,
                measure_units = measure_units,
                value = value
            )
            session.add(unemployment_rate_instance)
            session.commit()

if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(UnemploymentRateSpider)
    process.start()