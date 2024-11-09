import pymysql
from sqlalchemy import create_engine
import pandas as pd
from datetime import date, timedelta
import requests as rq
import time
from tqdm import tqdm
from io import BytesIO

# 데이터베이스 엔진 및 커넥션 설정
engine = create_engine('mysql+pymysql://root:7963@127.0.0.1:3306/stock_db')
con = pymysql.connect(
    user='root',
    passwd='7963',
    host='127.0.0.1',
    db='stock_db',
    charset='utf8'
)
mycursor = con.cursor()

# 주식 목록 가져오기 쿼리
query = """
    SELECT * FROM kor_ticker
    WHERE 기준일 = (SELECT MAX(기준일) FROM kor_ticker)
      AND 종목구분 = '보통주';
"""

# 티커 목록 불러오기
ticker_list = pd.read_sql(query, con=engine)

# 주식 가격 삽입 및 업데이트 쿼리
query = """
    INSERT INTO kor_price (날짜, 시가, 고가, 저가, 종가, 거래량, 종목코드)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    시가 = VALUES(시가), 고가 = VALUES(고가), 저가 = VALUES(저가), 종가 = VALUES(종가), 거래량 = VALUES(거래량);
"""

# 시작 날짜와 끝 날짜 설정
# 시작 날짜 : kor_price 테이블에서 가장 최근 날짜 + 1일
recent_date_query = "SELECT MAX(날짜) FROM kor_price"
mycursor.execute(recent_date_query)
recent_date = mycursor.fetchone()[0]

if recent_date:
    start_date = (recent_date + timedelta(days=1)).strftime("%Y%m%d")
else:
    # 최근 날짜가 없을 경우 기본 시작 날짜 설정
    start_date = (date.today() - timedelta(days=5*365)).strftime("%Y%m%d")  # 5년 전

# 끝 날짜 : 오늘 날짜
end_date = date.today().strftime("%Y%m%d")

# 에러 목록 초기화
error_list = []

# 티커별 데이터 수집 및 DB 삽입
for i in tqdm(range(len(ticker_list))):
    ticker = ticker_list['종목코드'][i]

    try:
        # 데이터 URL 생성 및 요청
        url = f'https://m.stock.naver.com/front-api/external/chart/domestic/info?symbol={ticker}&requestType=1&startTime={start_date}&endTime={end_date}&timeframe=day'
        response = rq.get(url)
        
        if response.status_code == 200:
            # 데이터 읽기
            data_price = pd.read_csv(BytesIO(response.content))

            # 데이터 정리
            price = data_price.iloc[:, 0:6]
            price.columns = ['날짜', '시가', '고가', '저가', '종가', '거래량']
            price = price.dropna()
            price['날짜'] = pd.to_datetime(price['날짜'].str.extract('(\d+)')[0])
            price['종목코드'] = ticker

            # 데이터베이스 삽입
            args = price.values.tolist()
            mycursor.executemany(query, args)
            con.commit()
        else:
            print(f"Failed to retrieve data for ticker {ticker}")
            error_list.append(ticker)

    except Exception as e:
        print(f"Error with ticker {ticker}: {e}")
        error_list.append(ticker)
    
    # API 호출 제한을 피하기 위한 지연
    time.sleep(2)

# 데이터베이스 연결 종료
engine.dispose()
con.close()

print("Errors:", error_list)
