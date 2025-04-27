# 📦 필요한 패키지
import pandas as pd
import numpy as np
from prophet import Prophet
import json
from typing import List, Dict
from datetime import datetime, timedelta

# 방문자수 데이터 불러오기 (송파나루공원만 필터링)
def load_visitor_data(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath)
    df = df[['측정시간', '방문자수', '공원명']].copy() 
    df = df[df['공원명'] == '송파나루공원'] 
    df.rename(columns={'측정시간': 'ds', '방문자수': 'y'}, inplace=True)
    df['ds'] = pd.to_datetime(df['ds'])
    return df

# 공휴일 데이터
def load_holidays(filepath: str) -> pd.DataFrame:
    holidays_df = pd.read_csv(filepath)
    holidays_df.rename(columns={'date': 'ds', 'holiday': 'holiday'}, inplace=True)
    holidays_df['ds'] = pd.to_datetime(holidays_df['ds'])
    return holidays_df[['ds', 'holiday']]

# Prophet 모델 생성 
def build_prophet_model(holidays: pd.DataFrame) -> Prophet:
    model = Prophet(
        daily_seasonality=True,
        weekly_seasonality=True,
        yearly_seasonality=False,
        holidays=holidays,
        seasonality_mode='additive' 
    )
    return model

# 모델 학습 및 예측
def train_and_forecast(df: pd.DataFrame, model: Prophet, predict_hours: int) -> pd.DataFrame:
    model.fit(df)
    future = model.make_future_dataframe(periods=predict_hours, freq='H')
    forecast = model.predict(future)
    forecast['yhat'] = forecast['yhat'].clip(lower=0)
    forecast['yhat_lower'] = forecast['yhat_lower'].clip(lower=0)
    forecast['yhat_upper'] = forecast['yhat_upper'].clip(lower=0)
    return forecast

# 예측 결과 JSON 포맷 변환
def format_forecast_to_json(forecast_df: pd.DataFrame, start_date: str, end_date: str) -> List[Dict]:

    forecast_df['date'] = forecast_df['ds'].dt.date
    forecast_df['hour'] = forecast_df['ds'].dt.hour

    target_dates = pd.date_range(start=start_date, end=end_date).date
    result = []

    for date in target_dates:
        df_day = forecast_df[forecast_df['date'] == date]
        day_data = {'day': date.strftime('%Y-%m-%d')}
        for hour in range(24):
            value = df_day[df_day['hour'] == hour]['yhat']
            if not value.empty:
                day_data[hour] = round(value.values[0], 2)
            else:
                day_data[hour] = 0
        result.append(day_data)

    return result

# JSON 파일 저장
def save_json(data: List[Dict], output_path: str) -> None:
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def main():
    # 파일 경로 설정
    visitor_data_path = 'dataset/park_data.csv'
    holiday_data_path = 'dataset/kr_holidays_2023_2025.csv'
    output_json_path = 'output/visitor_forecast.json'

    # 데이터 로드
    df = load_visitor_data(visitor_data_path)
    holidays = load_holidays(holiday_data_path)

    # Prophet 모델 생성
    model = build_prophet_model(holidays)

    # 학습 및 예측
    forecast = train_and_forecast(df, model, predict_hours=45*24)

    # 오늘 기준으로 날짜 자동 계산
    today = datetime.today().date()
    start_date = today + timedelta(days=1)
    end_date = start_date + timedelta(days=6)
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    # JSON 포맷 변환
    forecast_json = format_forecast_to_json(forecast, start_date=start_date_str, end_date=end_date_str)

    # 저장
    save_json(forecast_json, output_json_path)
    print(f"저장 완료 {start_date_str} ~ {end_date_str} 예측 저장: {output_json_path}")

if __name__ == '__main__':
    main()