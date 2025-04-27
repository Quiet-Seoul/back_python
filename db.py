import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return pymysql.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        db=os.getenv('DB_NAME'),
        charset='utf8'
    )

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
        ppltn_time = VALUES(ppltn_time),
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
