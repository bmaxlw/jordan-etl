# Date: 08/08/22
# Used to extract and enumerate a text-data from a web-page.
# (!) Parser is built to extract specific html-elements.

import scrapy
from scrapy.crawler import CrawlerProcess

class AnySpider(scrapy.Spider):
    name = "any-page"
    start_urls = ['https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)',]
    global elements_to_parse
    # 'td::text' is replacable w/ any html element (e.g.: 'title::text')
    elements_to_parse = 'td::text'

    def parse(self, response):
        response_list = response.css(elements_to_parse).getall()
        idx = 0
        with open('web-page.txt', 'a') as file:
            for element in response_list:
                file.write(f'<{idx}> {str(element).strip()}\n')
                idx+=1

if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(AnySpider)
    process.start()