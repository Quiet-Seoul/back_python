import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

BASE_URL = 'http://openapi.seoul.go.kr:8088'
API_KEY = os.getenv('SEOUL_API_KEY')

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
