import re

from scrapy import Spider, Request, signals
from scrapy.linkextractors import LinkExtractor
from scrapy.loader.processors import MapCompose
from dateparser import parse as parse_date
from parsel import css2xpath

from benjamin_brattain_crawler.items import NewsItemLoader


strip_querystring = lambda s: s.split('?')[0]


class NewsSpider(Spider):
    name = 'news'
    sites = {
        'foxnews': {
            'url': 'http://www.foxnews.com/',
            'allowed_domains': ['foxnews.com'],
            'parsing_method': 'parse_fox_news_article',
            'navigation_css': ['nav#menu', 'nav#main-nav', 'nav#sub'],
            'news_url_regex': [r'foxnews.com/[\w-]+/\d{4}/\d{2}/\d{2}/[\w-]+.html\??'],
        },
        'washingtonpost': {
            'url': 'https://www.washingtonpost.com/',
            'allowed_domains': ['washingtonpost.com'],
            'parsing_method': 'parse_washington_post_article',
            'navigation_css': ['li.main-nav'],
            'news_url_regex': [
                r'washingtonpost.com/news/[\w-]+/wp/\d{4}/\d{2}/\d{2}/[\w-]+/\??',
                r'washingtonpost.com/[\w-]+/[\w-]+/\d{4}/\d{2}/\d{2}/[\w-]+_story.html\??',
            ],
        },
        'wsj': {
            'url': 'http://www.wsj.com/',
            'allowed_domains': ['wsj.com'],
            'parsing_method': 'parse_wsj_article',
            'navigation_css': ['nav.sectionFronts'],
            'news_url_regex': [r'wsj.com/articles/[\w-]+'],
        },
        'cnn': {
            'url': 'http://edition.cnn.com/',
            'allowed_domains': ['www.cnn.com', 'edition.cnn.com'],
            'parsing_method': 'parse_cnn_article',
            'navigation_css': ['div.nav-menu-links'],
            'news_url_regex': [
                r'cnn.com/\d{4}/\d{2}/\d{2}/[\w-]+/[\w-]+/',
                r'cnn.com/\d{4}/\d{2}/\d{2}/[\w-]+/[\w-]+/index.html',
            ],
        },
    }

    custom_settings = {
        'EXTENSIONS': {
            'benjamin_brattain_crawler.extensions.CrawlLimitExtension': 100,
        },
    }

    # arguments to be owerwritten
    crawl_site = 'foxnews'
    crawl_limit = 50  # a zero value sets no limit

    def __init__(self, *args, **kwargs):
        super(NewsSpider, self).__init__(*args, **kwargs)
        self.crawl_site = self.crawl_site.strip().lower()
        try:
            current_site = self.sites[self.crawl_site]
            self.logger.info('Crawling site: {} (url: {})'.format(self.crawl_site, current_site['url']))
        except KeyError:
            self.logger.error('Unrecognized site: {}'.format(self.crawl_site))
        else:
            # set site data as spider attributes
            for key, value in current_site.items():
                setattr(self, key, value)
            self.crawl_limit = int(self.crawl_limit)
            self.start_urls = [current_site['url']]
            self.parsing_method = getattr(self, current_site['parsing_method'])

    def _extract_links(self, response, **kwargs):
        """ General method to extract links (avoid creating link extractors each time) """
        le = LinkExtractor(process_value=strip_querystring, **kwargs)
        links = set([l.url for l in le.extract_links(response)])
        links.discard(response.url)
        return links

    def _extract_article_links(self, response, **kwargs):
        return self._extract_links(
            response,
            allow_domains=self.allowed_domains,
            allow=self.news_url_regex,
            **kwargs
        )

    def _extract_external_links(self, response, **kwargs):
        return self._extract_links(
            response,
            deny_domains=self.allowed_domains,
            **kwargs
        )

    def _extract_navigation_links(self, response, **kwargs):
        return self._extract_links(
            response,
            allow_domains=self.allowed_domains,
            restrict_css=self.navigation_css,
            **kwargs
        )

    def _create_loader(self, response, **fields):
        """ Create an item loader pre-loaded with common values """
        l = NewsItemLoader(response=response)
        for key, value in fields.items():
            l.add_value(key, value)
        l.add_value('source_site', self.crawl_site)
        l.add_value('canonical_url', strip_querystring(response.url))
        l.add_value('referrer_url', response.request.headers.get('Referer'))
        return l

    def parse(self, response):
        meta = {}  # empty meta by default
        # CNN site loads sections using additional requests
        if self.crawl_site == 'cnn':
            import js2xml
            text = response.xpath('//script[contains(., "CNN.Zones")]/text()').extract_first()
            parsed = js2xml.parse(text)
            paths = set(parsed.xpath('//property[@name="zones"]//string/text()'))
            zone_url = 'http://edition.cnn.com/data/ocs/section/{}/views/zones/common/zone-manager.html'
            links = [zone_url.format(l) for l in paths]
            meta = {'referrer_url': response.url}  # overwrite referrer URL
            for url in links:
                yield Request(url, callback=self.parse_cnn_zone_manager, meta=meta,
                              headers={'X-Requested-With': 'XMLHttpRequest'})

        # follow navigation links
        for url in self._extract_navigation_links(response):
            yield Request(url, callback=self.parse, meta=meta)
        # extract news links
        for url in self._extract_article_links(response):
            yield Request(url, callback=self.parsing_method, meta=meta, priority=10)

    def parse_fox_news_article(self, response):
        l = self._create_loader(response)
        l.add_xpath('news_title', '//h1[@itemprop="headline"]/text()')
        l.add_css('news_title', 'div.main h1::text')
        l.add_css('news_content', 'div.article-text > *')
        l.add_xpath('news_date', '//time[@itemprop="datePublished"]/@datetime')
        l.add_xpath('news_date', '//time[@pubdate]/@datetime')
        # links
        internal = self._extract_article_links(response)
        external = self._extract_external_links(response, restrict_css='div.article-text')
        l.add_value('total_links_number', internal.union(external))
        l.add_value('outgoing_internal_links', internal)
        # output current item
        yield l.load_item()
        # follow additional internal news links
        for url in internal:
            yield Request(url, callback=self.parsing_method)

    def parse_washington_post_article(self, response):
        l = self._create_loader(response)
        l.add_xpath('news_title', '//h1[@itemprop="headline"]/text()')
        l.add_xpath('news_content', '//article[@itemprop="articleBody"]/*')
        l.add_xpath('news_date', '//span[@itemprop="datePublished"]/@content')
        # links
        internal = self._extract_article_links(response)
        external = self._extract_external_links(response, restrict_xpaths='//article[@itemprop="articleBody"]')
        l.add_value('total_links_number', internal.union(external))
        l.add_value('outgoing_internal_links', internal)
        # output current item
        yield l.load_item()
        # follow additional internal news links
        for url in internal:
            yield Request(url, callback=self.parsing_method)

    def parse_wsj_article(self, response):
        l = self._create_loader(response)
        l.add_xpath('news_title', '//h1[@itemprop="headline"]/text()')
        l.add_css('news_date', 'time.timestamp::text', MapCompose(
            lambda s: re.sub('updated', '', s, flags=re.I), parse_date, lambda d: d.isoformat()
        ))
        # news content
        if response.xpath('//div[@itemprop="articleBody"]'):
            xpath = '//div[@itemprop="articleBody"]/' + css2xpath('.byline-wrap') + '/following-sibling::*'
        else:
            xpath = '//article[.//h1]/div[1]//time/following-sibling::div[1]/*'
        l.add_xpath('news_content', xpath)
        # links
        internal = self._extract_article_links(response)
        external = self._extract_external_links(response, restrict_xpaths=xpath)
        l.add_value('total_links_number', internal.union(external))
        l.add_value('outgoing_internal_links', internal)
        # output current item
        yield l.load_item()
        # follow additional internal news links
        for url in internal:
            yield Request(url, callback=self.parsing_method)

    def parse_cnn_zone_manager(self, response):
        meta = {'referrer_url': response.meta['referrer_url']}
        # follow navigation links
        for url in self._extract_navigation_links(response):
            yield Request(url, callback=self.parse, meta=meta)
        # request news
        for url in self._extract_article_links(response):
            yield Request(url, callback=self.parsing_method, meta=meta, priority=10)

    def parse_cnn_article(self, response):
        l = self._create_loader(response, referrer_url=response.meta.get('referrer_url'))
        l.add_css('news_title', 'h1.pg-headline::text')
        l.add_css('news_content', 'section#body-text .l-container > *')
        l.add_css('news_date', 'p.update-time::text',
                  MapCompose(parse_date, lambda d: d.isoformat()),
                  re=r'Updated \d{4} GMT \(\d{4} HKT\) (.*)')
        # links
        internal = self._extract_article_links(response)
        external = self._extract_external_links(response, restrict_css='section#body-text .l-container')
        l.add_value('total_links_number', internal.union(external))
        l.add_value('outgoing_internal_links', internal)
        # output current item
        yield l.load_item()
        # follow additional internal news links
        for url in internal:
            yield Request(url, callback=self.parsing_method)
