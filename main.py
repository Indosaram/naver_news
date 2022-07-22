import datetime

from naver_news import NaverNewsCrawler

if __name__ == '__main__':
    crawler = NaverNewsCrawler()

    keyword = '마이데이터'
    start_date = "2020-06-19"
    end_date = "2020-06-21"

    
    data = crawler.run(keyword, start_date, end_date)

    today = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    data.to_csv(f"{keyword}_{today}_naver.csv", encoding='utf-8-sig')
    print(f"Succeed to save {keyword}")
