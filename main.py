import os
from dotenv import load_dotenv
import requests
import xml.etree.ElementTree as ET
import pymysql
from datetime import datetime

load_dotenv()

BASE_URL = 'http://openapi.seoul.go.kr:8088'
API_KEY = os.getenv('SEOUL_API_KEY')

def get_connection():
    return pymysql.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        db=os.getenv('DB_NAME'),
        charset='utf8'
    )

def fetch_area_data(area_nm):
    url = f"{BASE_URL}/{API_KEY}/xml/citydata/1/5/{area_nm}"
    response = requests.get(url)
    root = ET.fromstring(response.content)

    citydata = root.find('CITYDATA')
    live = citydata.find('.//LIVE_PPLTN_STTS/')

    if live is None or citydata.findtext('AREA_CD') is None or live.findtext('PPLTN_TIME') is None:
        return None, [], []

    area_cd = citydata.findtext('AREA_CD')
    ppltn_time = live.findtext('PPLTN_TIME')
    created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    live_data = {
        'area_cd': area_cd,
        'ppltn_time': ppltn_time,
        'area_congest_lvl': live.findtext('AREA_CONGEST_LVL'),
        'area_congest_msg': live.findtext('AREA_CONGEST_MSG'),
        'area_ppltn_min': live.findtext('AREA_PPLTN_MIN'),
        'area_ppltn_max': live.findtext('AREA_PPLTN_MAX'),
        'area_cmrcl_lvl': None,
        'area_sh_payment_cnt': None,
        'area_sh_payment_amt_min': None,
        'area_sh_payment_amt_max': None,
        'created_at': created_at
    }

    commercial = citydata.find('.//LIVE_CMRCL_STTS')
    if commercial is not None:
        live_data['area_cmrcl_lvl'] = commercial.findtext('AREA_CMRCL_LVL')
        live_data['area_sh_payment_cnt'] = commercial.findtext('AREA_SH_PAYMENT_CNT')
        live_data['area_sh_payment_amt_min'] = commercial.findtext('AREA_SH_PAYMENT_AMT_MIN')
        live_data['area_sh_payment_amt_max'] = commercial.findtext('AREA_SH_PAYMENT_AMT_MAX')

    fcst_data = []
    fcst_list = live.findall(".//FCST_PPLTN/FCST_PPLTN")
    for slot, fcst in enumerate(fcst_list[:12], start=1):
        fcst_data.append({
            'area_cd': area_cd,
            'slot': slot,
            'fcst_time': fcst.findtext('FCST_TIME'),
            'fcst_congest_lvl': fcst.findtext('FCST_CONGEST_LVL'),
            'fcst_ppltn_min': fcst.findtext('FCST_PPLTN_MIN'),
            'fcst_ppltn_max': fcst.findtext('FCST_PPLTN_MAX'),
            'created_at': created_at
        })

    industry_data = []
    if commercial is not None:
        for rsb in commercial.findall('.//CMRCL_RSB/CMRCL_RSB'):
            industry_data.append({
                'area_cd': area_cd,
                'rsb_lrg_ctgr': rsb.findtext('RSB_LRG_CTGR'),
                'rsb_mid_ctgr': rsb.findtext('RSB_MID_CTGR'),
                'rsb_payment_lvl': rsb.findtext('RSB_PAYMENT_LVL'),
                'rsb_sh_payment_cnt': rsb.findtext('RSB_SH_PAYMENT_CNT'),
                'rsb_sh_payment_amt_min': rsb.findtext('RSB_SH_PAYMENT_AMT_MIN'),
                'rsb_sh_payment_amt_max': rsb.findtext('RSB_SH_PAYMENT_AMT_MAX'),
                'created_at': created_at
            })

    return live_data, fcst_data, industry_data

def insert_live_data(data):
    conn = get_connection()
    cursor = conn.cursor()
    sql = """
    INSERT INTO area_data_live (
        area_cd, ppltn_time, area_congest_lvl, area_congest_msg,
        area_ppltn_min, area_ppltn_max, area_cmrcl_lvl, area_sh_payment_cnt,
        area_sh_payment_amt_min, area_sh_payment_amt_max, created_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        area_congest_lvl = VALUES(area_congest_lvl),
        area_congest_msg = VALUES(area_congest_msg),
        area_ppltn_min = VALUES(area_ppltn_min),
        area_ppltn_max = VALUES(area_ppltn_max),
        area_cmrcl_lvl = VALUES(area_cmrcl_lvl),
        area_sh_payment_cnt = VALUES(area_sh_payment_cnt),
        area_sh_payment_amt_min = VALUES(area_sh_payment_amt_min),
        area_sh_payment_amt_max = VALUES(area_sh_payment_amt_max),
        created_at = VALUES(created_at)
    """
    cursor.execute(sql, (
        data['area_cd'], data['ppltn_time'], data['area_congest_lvl'], data['area_congest_msg'],
        data['area_ppltn_min'], data['area_ppltn_max'], data['area_cmrcl_lvl'],
        data['area_sh_payment_cnt'], data['area_sh_payment_amt_min'], data['area_sh_payment_amt_max'],
        data['created_at']
    ))
    conn.commit()
    cursor.close()
    conn.close()

def insert_fcst_data(fcst_list):
    conn = get_connection()
    cursor = conn.cursor()
    sql = """
    INSERT INTO area_data_fcst (area_cd, slot, fcst_time, fcst_congest_lvl, fcst_ppltn_min, fcst_ppltn_max, created_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    fcst_time = VALUES(fcst_time),
    fcst_congest_lvl = VALUES(fcst_congest_lvl),
    fcst_ppltn_min = VALUES(fcst_ppltn_min),
    fcst_ppltn_max = VALUES(fcst_ppltn_max),
    created_at = VALUES(created_at)
    """
    for item in fcst_list:
        cursor.execute(sql, (
            item['area_cd'], item['slot'], item['fcst_time'],
            item['fcst_congest_lvl'], item['fcst_ppltn_min'], item['fcst_ppltn_max'], item['created_at']
        ))
    conn.commit()
    cursor.close()
    conn.close()

def insert_industry_data(industry_list):
    conn = get_connection()
    cursor = conn.cursor()
    sql = """
    INSERT INTO area_industry_live (area_cd, rsb_lrg_ctgr, rsb_mid_ctgr, rsb_payment_lvl, rsb_sh_payment_cnt,
    rsb_sh_payment_amt_min, rsb_sh_payment_amt_max, created_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    rsb_payment_lvl = VALUES(rsb_payment_lvl),
    rsb_sh_payment_cnt = VALUES(rsb_sh_payment_cnt),
    rsb_sh_payment_amt_min = VALUES(rsb_sh_payment_amt_min),
    rsb_sh_payment_amt_max = VALUES(rsb_sh_payment_amt_max),
    created_at = VALUES(created_at)
    """
    for item in industry_list:
        cursor.execute(sql, (
            item['area_cd'], item['rsb_lrg_ctgr'], item['rsb_mid_ctgr'],
            item['rsb_payment_lvl'], item['rsb_sh_payment_cnt'],
            item['rsb_sh_payment_amt_min'], item['rsb_sh_payment_amt_max'], item['created_at']
        ))
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == '__main__':
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT area_nm FROM area")
    area_list = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()

    for area in area_list:
        print(f"크롤링 중: {area}")
        live_data, fcst_data, industry_data = fetch_area_data(area)
        if live_data:
            insert_live_data(live_data)
        if fcst_data:
            insert_fcst_data(fcst_data)
        if industry_data:
            insert_industry_data(industry_data)