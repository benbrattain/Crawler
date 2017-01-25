# Crawler
Crawler for news orgs that scrapes pages daily for Search Engine.

This is a crawler that will scrape new articles from 1). The Washington Post 2). The Wall Street Journal 3). Fox News 4). CNN.

This code will be run on a cluster and scrape the new articles (only the delta from day to day) every day so my search engine can pull the data from these articles.

The rest of the search engine can be found here (still a work in progress): https://github.com/benbrattain/Search-Engine

This crawler currently captures the following:
1). All of the text in the article
2). All of the outgoing internal links that we're following (for PageRank later).
3). The link the pointed to this article.
4). Total number of links on the page.
5). The url of the article itself.
