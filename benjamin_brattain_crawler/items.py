# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import re

from scrapy.item import Item, Field
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, Identity, Join, Compose, MapCompose
from w3lib.html import remove_tags, remove_tags_with_content, remove_comments


def clean_html(html):
    # remove comments
    html = remove_comments(html)
    # remove some tags (and content)
    tags = ('script', 'noscript', 'style', 'iframe', 'link')
    html = remove_tags_with_content(html, which_ones=tags)
    # remove 'data-*' attributes (and content)
    html = re.sub(r'\sdata-[\w-]+=".*?"', '', html)
    # remove other attributes (and content)
    attributes = (
        'class', 'id', 'alt', 'data-analytics', 'target',
        'itemprop', 'name', 'dir', 'lang', 'style',
    )
    for attr in attributes:
        html = re.sub(r'\s{}=".*?"'.format(attr), '', html)
    # remove empty divs
    html = re.sub(r'<div>\s*</div>', '', html)
    # remove unnecessary whitespaces (3 or more)
    html = re.sub(r'\s{3,}', '', html)
    return html

class NewsItem(Item):
    source_site = Field()
    canonical_url = Field()
    referrer_url = Field()
    news_title = Field()
    news_content = Field(input_processor=Compose(Join(), clean_html))
    news_date = Field(input_processor=MapCompose(lambda date: date[:10]))
    outgoing_internal_links = Field(output_processor=Compose(list))
    total_links_number = Field(input_processor=Compose(set, len))


class NewsItemLoader(ItemLoader):
    default_item_class = NewsItem
    default_output_processor = TakeFirst()
