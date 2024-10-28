import requests as rq
from bs4 import BeautifulSoup
import re

url = 'https://finance.naver.com/sise/sise_deposit.naver'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' \
                  'AppleWebKit/537.36 (KHTML, like Gecko) ' \
                  'Chrome/58.0.3029.110 Safari/537.3'}

try:
    response = rq.get(url, headers=headers)
    response.raise_for_status()  # 잘못된 응답에 대해 HTTPError를 발생시킵니다
    data_html = BeautifulSoup(response.content, 'html.parser')
    element = data_html.select_one(
        'div.subtop_sise_graph2 > ul.subtop_chart_note > li > span.tah')
    
    if element:
        parse_day = element.text
        biz_day = ''.join(re.findall('[0-9]+', parse_day))
        print(f"영업일: {biz_day}")
    else:
        print("지정된 요소를 HTML에서 찾을 수 없습니다.")
except rq.exceptions.RequestException as e:
    print(f"데이터를 가져오는 중 오류가 발생했습니다: {e}")
except Exception as e:
    print(f"예상치 못한 오류가 발생했습니다: {e}")

from io import BytesIO
import pandas as pd

# 코스피
gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
gen_otp_stk = {
    'mktId': 'STK',
    'trdDd': biz_day,
    'money': '1',
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT03901'
}


headers = {
    'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

otp_stk = rq.post(gen_otp_url, gen_otp_stk, headers=headers).text

down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
down_sector_stk = rq.post(down_url, {'code' : otp_stk}, headers=headers)
sector_stk = pd.read_csv(BytesIO(down_sector_stk.content), encoding = 'EUC-KR')

# 코스닥
gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
gen_otp_ksq = {
    'mktId': 'KSQ',
    'trdDd': biz_day,
    'money': '1',
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT03901'
}


headers = {
    'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

otp_ksq = rq.post(gen_otp_url, gen_otp_ksq, headers=headers).text

down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
down_sector_ksq = rq.post(down_url, {'code' : otp_ksq}, headers=headers)
sector_ksq = pd.read_csv(BytesIO(down_sector_ksq.content), encoding = 'EUC-KR')

krx_sector = pd.concat([sector_stk, sector_ksq]).reset_index(drop = True)
krx_sector['종목명'] = krx_sector['종목명'].str.strip()
krx_sector['기준일'] = biz_day


# 개별종목
gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
gen_otp_ind = {    
    'searchType': '1',
    'mktId': 'ALL',
    'trdDd': biz_day,
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT03501'
}


headers = {
    'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

otp_ind = rq.post(gen_otp_url, gen_otp_ind, headers=headers).text

down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
down_sector_ind = rq.post(down_url, {'code' : otp_ind}, headers=headers)
krx_ind = pd.read_csv(BytesIO(down_sector_ind.content), encoding = 'EUC-KR')

krx_ind['종목명'] = krx_ind['종목명'].str.strip()
krx_ind['기준일'] = biz_day



kor_ticker = pd.merge(krx_sector,
                      krx_ind,
                      on=krx_sector.columns.intersection(
                          krx_ind.columns).tolist(),
                      how='outer')

kor_ticker[kor_ticker['종목명'].str.contains('스펙|제[0-9]+호')]['종목명']
kor_ticker[kor_ticker['종목코드'].str[-1:] != '0']['종목명']
kor_ticker[kor_ticker['종목명'].str.endswith('리츠')]['종목명']

import numpy as np

diff = list(set(krx_sector['종목명']).symmetric_difference(set(krx_ind['종목명'])))

kor_ticker['종목구분'] = np.where(kor_ticker['종목명'].str.contains('스펙|제[0-9]+호'), '스펙',
                             np.where(kor_ticker['종목코드'].str[-1:] != '0', '우선주',
                             np.where(kor_ticker['종목명'].str.endswith('리츠'), '리츠',
                             np.where(kor_ticker['종목명'].isin(diff), '기타',
                                      '보통주'))))

kor_ticker = kor_ticker.reset_index(drop=True)
kor_ticker.columns = kor_ticker.columns.str.replace(' ','')
kor_ticker = kor_ticker[['종목코드', '종목명', '시장구분', '종가', '시가총액', '기준일',
                         'EPS', '선행EPS', 'BPS', '주당배당금', '종목구분']]
kor_ticker = kor_ticker.replace({np.nan:None})

import pymysql
                         
con = pymysql.connect(
    user = 'root',
    passwd = '7963',
    host = '127.0.0.1',
    db = 'stock_db',
    charset = 'utf8'
)

mycursor = con.cursor()

query = f"""
    insert into kor_ticker (종목코드, 종목명, 시장구분, 종가, 시가총액, 기준일, EPS, 선행EPS, BPS, 주당배당금, 종목구분)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) as new
    on duplicate key update
    종목명=new.종목명, 시장구분=new.시장구분, 종가=new.종가, 시가총액=new.시장구분, EPS=new.EPS, 선행EPS=new.선행EPS,
    BPS=new.BPS, 주당배당금=new.주당배당금, 종목구분=new.종목구분;
"""

# query = """
#     INSERT INTO kor_ticker (종목코드, 종목명, 시장구분, 종가, 시가총액, 기준일, EPS, 선행EPS, BPS, 주당배당금, 종목구분)
#     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#     ON DUPLICATE KEY UPDATE
#         종목명=VALUES(종목명), 
#         시장구분=VALUES(시장구분), 
#         종가=VALUES(종가), 
#         시가총액=VALUES(시가총액), 
#         EPS=VALUES(EPS), 
#         선행EPS=VALUES(선행EPS),
#         BPS=VALUES(BPS), 
#         주당배당금=VALUES(주당배당금), 
#         종목구분=VALUES(종목구분);
# """    

args = kor_ticker.values.tolist()
mycursor.executemany(query, args)
con.commit()

con.close()


import json
import requests as rq
import pandas as pd

url = f'''https://www.wiseindex.com/Index/GetIndexComponets?ceil_yn=0&dt={biz_day}&sec_cd=G10'''
data = rq.get(url).json()
