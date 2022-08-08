# Date: 08/08/22
# Frequency: Daily

import scrapy
from sqlalchemy.orm import Session
from database import database as DB
from database import connector as CN
from scrapy.crawler import CrawlerProcess

class InterestRateSpider(scrapy.Spider):
    name = "interest-rate"
    start_urls = ['https://tradingeconomics.com/jordan/interest-rate',]
    engine = CN.Connector().connect() # modifiable, default args used

    def parse(self, response):
        response_list = response.css('td::text').getall()
        frequency = str(response_list[60]).strip()
        start_date = str(response_list[5]).strip()
        measure_units = str(response_list[59]).strip()
        value = str(response_list[54]).strip()

        with Session(self.engine) as session:
            interest_rate_instance = DB.Indicator(
                indicator_name = 'Interest Rate',
                category = 'Money',
                frequency = frequency,
                start_date = start_date,
                measure_units = measure_units,
                value = value
            )
            session.add(interest_rate_instance)
            session.commit()

if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(InterestRateSpider)
    process.start()
