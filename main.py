from db import get_connection, insert_live_data, insert_fcst_data, insert_industry_data
from live_crawler import fetch_area_data

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
