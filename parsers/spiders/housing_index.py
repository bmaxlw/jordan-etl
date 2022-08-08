# Date: 08/08/22
# Frequency: Quarterly

import scrapy
from sqlalchemy.orm import Session
from database import database as DB
from database import connector as CN
from scrapy.crawler import CrawlerProcess

class HousingIndexSpider(scrapy.Spider):
    name = "housing-index"
    start_urls = ['https://tradingeconomics.com/jordan/housing-index',]
    engine = CN.Connector().connect() # modifiable, default args used

    def parse(self, response):
        response_list = response.css('td::text').getall()
        frequency = str(response_list[12]).strip()
        start_date = str(response_list[5]).strip()
        measure_units = str(response_list[4]).strip()
        value = str(response_list[6]).strip()

        with Session(self.engine) as session:
            housing_index_instance = DB.Indicator(
                indicator_name = 'Housing Index',
                category = 'Housing',
                frequency = frequency,
                start_date = start_date,
                measure_units = measure_units,
                value = value
            )
            session.add(housing_index_instance)
            session.commit()

if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(HousingIndexSpider)
    process.start()
