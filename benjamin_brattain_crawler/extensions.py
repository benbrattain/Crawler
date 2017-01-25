from scrapy import signals


class CrawlLimitExtension:
    """
    Closes the engine when the spider has extracted <spider.crawl_lmiit> items
    """

    item_count = 0

    def __init__(self, crawler):
        self.crawler = crawler
        self.crawler.signals.connect(self.spider_opened_handler, signals.spider_opened)
        self.crawler.signals.connect(self.item_scraped_handler, signals.item_scraped)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def spider_opened_handler(self, spider):
        msg = 'Crawl limit set to {}'.format(spider.crawl_limit)
        if spider.crawl_limit <= 0:
            msg += ' (no limit)'
        spider.logger.info(msg)

    def item_scraped_handler(self, item, response, spider):
        self.item_count += 1
        if spider.crawl_limit == self.item_count:
            spider.logger.info('Crawl limit reached ({})'.format(spider.crawl_limit))
            self.crawler.engine.close_spider(spider, 'crawl_limit_reached')
