import time
from typing import Optional, Set

import requests
import pandas as pd
from bs4 import BeautifulSoup as BS
from tqdm import tqdm


class NaverNewsCrawler:
    def __init__(self):
        super().__init__()

    def _crawl(self, url: str) -> BS:
        """
        Given a url, return the BeautifulSoup object.
        """
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/39.0.2171.95 Safari/537.36"
        }
        res = requests.get(url, headers=headers)
        soup = BS(res.text, features="html.parser")
        return soup

    def _get_last_pagination(self, keyword, date) -> int:

        # Naver only provides articles up to 7000 -> start=7001
        url = (
            "https://search.naver.com/search.naver?"
            f"where=news&sm=tab_jum&query={keyword}"
            f"&ds={date}&de={date}&start=7001"
            f"&nso=so%3Ar%2Cp%3Afrom{date.replace('.','')}"
            f"to{date.replace('.','')}"
        )

        soup = self._crawl(url)
        pagination_selector = (
            "#main_pack > div.api_sc_page_wrap > div > div > a"
        )
        return int(soup.select(pagination_selector)[-1].text)

    def get_urls(
        self, keyword: str, start_date: str, end_date: str
    ) -> Set[str]:
        """
        Return result as dictionary
        start_date and end_date must be in the format of YYYY.MM.DD
        i.e. 2022-04-25
        """
        print("Collecting news url list")
        date_list = list(
            map(
                lambda x: x.strftime("%Y.%m.%d"),
                pd.date_range(start=start_date, end=end_date),
            )
        )
        urls = set()

        # iterate over date list
        for date in tqdm(date_list):
            pagination = self._get_last_pagination(keyword, date)
            for page in range(1, pagination + 1):
                page_url = (
                    "https://search.naver.com/search.naver?"
                    f"where=news&sm=tab_jum&query={keyword}"
                    f"&ds={date}&de={date}&start={page}"
                    f"&nso=so%3Ar%2Cp%3Afrom{date.replace('.','')}"
                    f"to{date.replace('.','')}"
                )

                soup = self._crawl(page_url)
                for article in soup.find_all("a"):
                    if (
                        article.text == "네이버뉴스"
                        and "sports" not in article["href"]
                    ):
                        urls.add(article["href"])

            time.sleep(5)
        return urls

    def _get_news_data(self, url: str) -> dict:
        """
        Return single news data as a dictionary.
        The dict has the following keys: url, text, title, and media.
        """
        soup = self._crawl(url)

        body_selector = "#newsct_article"
        tag = soup.select_one(body_selector)

        if tag is None:
            return {}
        body_text = tag.text.strip().replace("\n", "").replace("\t", "")

        title_selector = (
            "#ct > div.media_end_head.go_trans > div.media_end_head_title > h2"
        )
        title = soup.select_one(title_selector).text

        return {
            "url": url,
            "text": body_text,
            "title": title,
        }

    def run(
        self, keyword: str, start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        From the set of urls, scraping news data and return a pandas dataframe.
        First, scrape urls for each date. Second, iterate over urls to get data.

        Raise:
            ValueError: if the set of urls is empty.
            Exception: if unknown excpetion occurs.
        """
        data = []
        for url in tqdm(self.get_urls(keyword, start_date, end_date)):
            _data = self._get_news_data(url)
            if _data:
                data.append(_data)
            time.sleep(3)

        print("Crawling finished")
        return pd.DataFrame(data)
