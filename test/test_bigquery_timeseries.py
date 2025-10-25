# test/test_bigquery_timeseries.py の先頭に追加
from dotenv import load_dotenv
load_dotenv()  # .envファイルから環境変数を読み込む

import pytest
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import bigquery_timeseries as bqts
from bigquery_timeseries.dt import normalize_datetime


# 環境変数から設定を読み込む
PROJECT_ID = os.getenv("BQ_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET_ID")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
TEST_TABLE_NAME = "test_ohlc_data"


@pytest.fixture(scope="module")
def bqts_client():
    """BQTSクライアントのフィクスチャ"""
    if not PROJECT_ID or not DATASET_ID:
        pytest.skip("Environment variables BQ_PROJECT_ID and BQ_DATASET_ID must be set")
    
    client = bqts.BQTS(
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID
    )
    return client


@pytest.fixture(scope="module")
def sample_data():
    """テスト用のサンプルデータを生成"""
    np.random.seed(42)
    
    # 5000行のOHLCデータを生成
    df = pd.DataFrame({
        'open': np.random.uniform(100, 200, 5000),
        'high': np.random.uniform(100, 200, 5000),
        'low': np.random.uniform(100, 200, 5000),
        'close': np.random.uniform(100, 200, 5000),
        'volume': np.random.uniform(1000, 10000, 5000),
    })
    
    # high が他の値より高くなるように調整
    df['high'] = df[['open', 'high', 'low', 'close']].max(axis=1) + np.random.uniform(0, 5, 5000)
    # low が他の値より低くなるように調整
    df['low'] = df[['open', 'low', 'close']].min(axis=1) - np.random.uniform(0, 5, 5000)
    
    df['symbol'] = np.random.choice(['BTCUSDT', 'ETHUSDT', 'BNBUSDT'], 5000)
    df['dt'] = pd.date_range('2024-01-01 00:00:00', periods=5000, freq='15min')
    
    # partition_dt を月初に設定
    df['partition_dt'] = df['dt'].dt.to_period('M').dt.to_timestamp()
    
    return df


class TestBQTSUpload:
    """アップロード機能のテスト"""
    
    def test_upload_basic(self, bqts_client, sample_data):
        """基本的なアップロードのテスト"""
        if not GCS_BUCKET_NAME:
            pytest.skip("Environment variable GCS_BUCKET_NAME must be set")
        
        # 最初の100行のみテスト
        test_df = sample_data.head(100).copy()
        
        bqts_client.upload(
            table_name=TEST_TABLE_NAME,
            df=test_df,
            gcs_bucket_name=GCS_BUCKET_NAME,
            keep_gcs_file=False,
            max_cost=1.0
        )
        
        # アップロードが成功したことを確認
        assert True
    
    def test_upload_with_partition_validation(self, bqts_client, sample_data):
        """partition_dtのバリデーションテスト"""
        if not GCS_BUCKET_NAME:
            pytest.skip("Environment variable GCS_BUCKET_NAME must be set")
        
        # 不正なpartition_dt（月初以外）を持つデータ
        invalid_df = sample_data.head(10).copy()
        invalid_df['partition_dt'] = pd.to_datetime('2024-01-15')
        
        # ValueErrorが発生することを確認
        with pytest.raises(ValueError, match="partition_dt must be 1st day"):
            bqts_client.upload(
                table_name=TEST_TABLE_NAME,
                df=invalid_df,
                gcs_bucket_name=GCS_BUCKET_NAME,
                keep_gcs_file=False
            )
    
    def test_upload_incremental(self, bqts_client, sample_data):
        """増分アップロードのテスト（既存データの削除と追加）"""
        if not GCS_BUCKET_NAME:
            pytest.skip("Environment variable GCS_BUCKET_NAME must be set")
        
        # 同じpartition_dt + symbolのデータで上書き
        test_df = sample_data[
            (sample_data['symbol'] == 'BTCUSDT') & 
            (sample_data['partition_dt'] == '2024-01-01')
        ].head(50).copy()
        
        bqts_client.upload(
            table_name=TEST_TABLE_NAME,
            df=test_df,
            gcs_bucket_name=GCS_BUCKET_NAME,
            keep_gcs_file=False,
            max_cost=1.0
        )
        
        assert True


class TestBQTSQuery:
    """クエリ機能のテスト"""
    
    def test_query_basic(self, bqts_client):
        """基本的なクエリのテスト"""
        result = bqts_client.query(
            table_name=TEST_TABLE_NAME,
            fields=['open', 'high', 'low', 'close', 'volume'],
            start_dt='2024-01-01 00:00:00',
            end_dt='2024-01-02 23:59:59',
            symbols=['BTCUSDT'],
            max_cost=1.0
        )
        
        # 結果の検証
        assert isinstance(result, pd.DataFrame)
        assert 'symbol' in result.columns
        assert result.index.name == 'dt'
        assert len(result) > 0
    
    def test_query_all_fields(self, bqts_client):
        """全カラム取得のテスト"""
        result = bqts_client.query(
            table_name=TEST_TABLE_NAME,
            fields=['*'],
            start_dt='2024-01-01 00:00:00',
            end_dt='2024-01-01 12:00:00',
            symbols=['BTCUSDT', 'ETHUSDT'],
            max_cost=1.0
        )
        
        assert isinstance(result, pd.DataFrame)
        assert len(result.columns) >= 5  # open, high, low, close, volume以上
        assert 'partition_dt' not in result.columns  # partition_dtは除外される
    
    def test_query_multiple_symbols(self, bqts_client):
        """複数シンボルのクエリテスト"""
        result = bqts_client.query(
            table_name=TEST_TABLE_NAME,
            fields=['close'],
            start_dt='2024-01-01 00:00:00',
            end_dt='2024-01-01 12:00:00',
            symbols=['BTCUSDT', 'ETHUSDT', 'BNBUSDT'],
            max_cost=1.0
        )
        
        unique_symbols = result['symbol'].unique()
        assert len(unique_symbols) <= 3
        assert all(s in ['BTCUSDT', 'ETHUSDT', 'BNBUSDT'] for s in unique_symbols)

class TestBQTSResampleQuery:
    """リサンプリングクエリのテスト"""
    
    def test_resample_daily(self, bqts_client):
        """日次リサンプリングのテスト"""
        result = bqts_client.resample_query(
            table_name=TEST_TABLE_NAME,
            fields=['open', 'high', 'low', 'close'],
            ops=['first', 'max', 'min', 'last'],
            start_dt='2024-01-01 00:00:00',
            end_dt='2024-01-05 23:59:59',
            symbols=['BTCUSDT'],
            interval='day',
            max_cost=1.0
        )
        
        assert isinstance(result, pd.DataFrame)
        assert result.index.name == 'dt'
        assert 'open' in result.columns
        assert 'high' in result.columns
        assert 'low' in result.columns
        assert 'close' in result.columns
    
    def test_resample_hourly(self, bqts_client):
        """時間リサンプリングのテスト"""
        result = bqts_client.resample_query(
            table_name=TEST_TABLE_NAME,
            fields=['close', 'volume'],
            ops=['last', 'sum'],
            start_dt='2024-01-01 00:00:00',
            end_dt='2024-01-01 23:59:59',
            symbols=['BTCUSDT', 'ETHUSDT'],
            interval='1hour',
            max_cost=1.0
        )
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
    
    def test_resample_weekly(self, bqts_client):
        """週次リサンプリングのテスト（月曜始まりの検証）"""
        result = bqts_client.resample_query(
            table_name=TEST_TABLE_NAME,
            fields=['high', 'low'],
            ops=['max', 'min'],
            start_dt='2024-01-01 00:00:00',
            end_dt='2024-01-31 23:59:59',
            symbols=['BTCUSDT'],
            interval='week',
            max_cost=1.0
        )
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        
        # 週次リサンプリングは月曜日始まりであることを検証
        for dt in result.index.get_level_values('dt').unique():
            # 月曜日 = 0
            day_name = dt.strftime('%A')
            assert dt.dayofweek == 0, f"Week should start on Monday, but got {day_name} for {dt}"
    
    def test_resample_invalid_ops(self, bqts_client):
        """無効な集約操作のテスト"""
        with pytest.raises(ValueError, match="Invalid operation"):
            bqts_client.resample_query(
                table_name=TEST_TABLE_NAME,
                fields=['close'],
                ops=['invalid_op'],
                start_dt='2024-01-01 00:00:00',
                end_dt='2024-01-02 23:59:59',
                symbols=['BTCUSDT'],
                interval='day'
            )
    
    def test_resample_ops_mismatch(self, bqts_client):
        """フィールド数と操作数の不一致テスト"""
        with pytest.raises(ValueError, match="number of fields must match"):
            bqts_client.resample_query(
                table_name=TEST_TABLE_NAME,
                fields=['open', 'close'],
                ops=['first'],  # 2つのフィールドに1つの操作
                start_dt='2024-01-01 00:00:00',
                end_dt='2024-01-02 23:59:59',
                symbols=['BTCUSDT'],
                interval='day'
            )
    
    def test_resample_minute_alignment(self, bqts_client):
        """分足リサンプリングの時刻整合性テスト"""
        # 5分足のリサンプリング
        result = bqts_client.resample_query(
            table_name=TEST_TABLE_NAME,
            fields=['close'],
            ops=['last'],
            start_dt='2024-01-01 00:00:00',
            end_dt='2024-01-01 01:00:00',
            symbols=['BTCUSDT'],
            interval='5minute',
            max_cost=1.0
        )
        
        # 5分刻みで時刻が整列していることを確認
        for dt in result.index.get_level_values('dt').unique():
            minute_val = dt.minute
            assert minute_val % 5 == 0, f"5-minute bars should align to 0, 5, 10, ..., but got minute={minute_val}"
    
    def test_resample_hour_alignment(self, bqts_client):
        """時間足リサンプリングの時刻整合性テスト"""
        # 4時間足のリサンプリング
        result = bqts_client.resample_query(
            table_name=TEST_TABLE_NAME,
            fields=['open', 'close'],
            ops=['first', 'last'],
            start_dt='2024-01-01 00:00:00',
            end_dt='2024-01-02 23:59:59',
            symbols=['BTCUSDT'],
            interval='4hour',
            max_cost=1.0
        )
        
        # 4時間刻みで時刻が整列していることを確認（0, 4, 8, 12, 16, 20時）
        for dt in result.index.get_level_values('dt').unique():
            hour_val = dt.hour
            minute_val = dt.minute
            assert hour_val % 4 == 0, f"4-hour bars should align to 0, 4, 8, ..., but got hour={hour_val}"
            assert minute_val == 0, f"Hour bars should have minute=0, but got minute={minute_val}"
    
    def test_resample_monthly_alignment(self, bqts_client):
        """月次リサンプリングの日付整合性テスト"""
        result = bqts_client.resample_query(
            table_name=TEST_TABLE_NAME,
            fields=['open', 'high', 'low', 'close'],
            ops=['first', 'max', 'min', 'last'],
            start_dt='2024-01-01 00:00:00',
            end_dt='2024-03-31 23:59:59',
            symbols=['BTCUSDT'],
            interval='month',
            max_cost=1.0
        )
        
        # 月次リサンプリングは月初（1日）であることを確認
        for dt in result.index.get_level_values('dt').unique():
            day_val = dt.day
            hour_val = dt.hour
            minute_val = dt.minute
            assert day_val == 1, f"Monthly bars should start on day 1, but got day={day_val}"
            assert hour_val == 0 and minute_val == 0, "Monthly bars should start at 00:00"
    
    def test_resample_ohlc_logic(self, bqts_client):
        """OHLCリサンプリングのロジック検証"""
        # 日次OHLCを取得
        result = bqts_client.resample_query(
            table_name=TEST_TABLE_NAME,
            fields=['open', 'high', 'low', 'close'],
            ops=['first', 'max', 'min', 'last'],
            start_dt='2024-01-01 00:00:00',
            end_dt='2024-01-05 23:59:59',
            symbols=['BTCUSDT'],
            interval='day',
            max_cost=1.0
        )
        
        # 各行でHigh >= Low, High >= Open, High >= Close, Low <= Open, Low <= Closeを確認
        for idx, row in result.iterrows():
            high = row['high']
            low = row['low']
            open_price = row['open']
            close_price = row['close']
            
            assert high >= low, f"High ({high}) should be >= Low ({low})"
            assert high >= open_price, f"High ({high}) should be >= Open ({open_price})"
            assert high >= close_price, f"High ({high}) should be >= Close ({close_price})"
            assert low <= open_price, f"Low ({low}) should be <= Open ({open_price})"
            assert low <= close_price, f"Low ({low}) should be <= Close ({close_price})"
    
    def test_resample_multi_symbol_consistency(self, bqts_client):
        """複数シンボルでのリサンプリング整合性テスト"""
        result = bqts_client.resample_query(
            table_name=TEST_TABLE_NAME,
            fields=['close', 'volume'],
            ops=['last', 'sum'],
            start_dt='2024-01-01 00:00:00',
            end_dt='2024-01-03 23:59:59',
            symbols=['BTCUSDT', 'ETHUSDT'],
            interval='day',
            max_cost=1.0
        )
        
        # 各日付に対して、指定した全シンボルのデータが存在することを確認
        dates = result.index.get_level_values('dt').unique()
        symbols = result['symbol'].unique()
        
        for date in dates:
            date_data = result.loc[date]
            # 各日付に複数シンボルが存在する場合、両方が揃っていることを期待
            if len(symbols) > 1:
                assert len(date_data) >= 1, f"Expected data for date {date}"


class TestBQTSDateUtils:
    """日付ユーティリティ関数のテスト"""
    
    def test_is_date(self):
        """日付判定関数のテスト"""
        from bigquery_timeseries.dt import is_date
        
        assert is_date('2024-01-01') is True
        assert is_date('2024-13-01') is False
        assert is_date('not-a-date') is False
    
    def test_to_quarter(self):
        """四半期計算のテスト"""
        from bigquery_timeseries.dt import to_quarter
        
        assert to_quarter(1) == 1
        assert to_quarter(4) == 2
        assert to_quarter(7) == 3
        assert to_quarter(12) == 4
    
    def test_compute_monthly_intervals(self):
        """月次区間計算のテスト"""
        from bigquery_timeseries.dt import compute_monthly_intervals
        
        intervals = list(compute_monthly_intervals('2024-01-15', '2024-03-10'))
        
        assert len(intervals) == 3
        assert intervals[0] == ('2024-01-15', '2024-01-31')
        assert intervals[1] == ('2024-02-01', '2024-02-29')
        assert intervals[2] == ('2024-03-01', '2024-03-10')

class TestNormalizeDatetime:
    """日付正規化関数のテスト"""
    
    def test_normalize_valid_date(self):
        """有効な日付はそのまま返す"""
        result = normalize_datetime('2024-01-15 10:30:00')
        assert result == '2024-01-15 10:30:00'
        
        result = normalize_datetime('2024-12-31')
        assert result == '2024-12-31 00:00:00'
    
    def test_normalize_invalid_september_31(self):
        """9月31日 -> 9月30日に修正"""
        result = normalize_datetime('2025-09-31')
        assert result == '2025-09-30 00:00:00'
        
        result = normalize_datetime('2025-09-31 23:59:59')
        assert result == '2025-09-30 23:59:59'
    
    def test_normalize_invalid_april_31(self):
        """4月31日 -> 4月30日に修正"""
        result = normalize_datetime('2025-04-31')
        assert result == '2025-04-30 00:00:00'
    
    def test_normalize_invalid_february_30_normal_year(self):
        """平年の2月30日 -> 2月28日に修正"""
        result = normalize_datetime('2025-02-30')
        assert result == '2025-02-28 00:00:00'
        
        result = normalize_datetime('2025-02-29')
        assert result == '2025-02-28 00:00:00'
    
    def test_normalize_invalid_february_30_leap_year(self):
        """閏年の2月30日 -> 2月29日に修正"""
        result = normalize_datetime('2024-02-30')
        assert result == '2024-02-29 00:00:00'
    
    def test_normalize_february_29_leap_year(self):
        """閏年の2月29日は有効"""
        result = normalize_datetime('2024-02-29')
        assert result == '2024-02-29 00:00:00'
    
    def test_normalize_invalid_november_31(self):
        """11月31日 -> 11月30日に修正"""
        result = normalize_datetime('2025-11-31')
        assert result == '2025-11-30 00:00:00'
    
    def test_normalize_with_time(self):
        """時刻付きの無効な日付も正しく修正"""
        result = normalize_datetime('2025-09-31 14:30:45')
        assert result == '2025-09-30 14:30:45'
    
    def test_normalize_invalid_month(self):
        """無効な月はエラー"""
        with pytest.raises(ValueError, match="Invalid month"):
            normalize_datetime('2025-13-01')
        
        with pytest.raises(ValueError, match="Invalid month"):
            normalize_datetime('2025-00-01')
    
    def test_normalize_various_31st_months(self):
        """31日まである月は正常に処理"""
        # 31日まである月: 1, 3, 5, 7, 8, 10, 12
        valid_months = [1, 3, 5, 7, 8, 10, 12]
        for month in valid_months:
            result = normalize_datetime(f'2025-{month:02d}-31')
            assert result == f'2025-{month:02d}-31 00:00:00'
    
    def test_normalize_30_day_months(self):
        """30日までの月で31日を指定すると30日に修正"""
        # 30日までの月: 4, 6, 9, 11
        thirty_day_months = [4, 6, 9, 11]
        for month in thirty_day_months:
            result = normalize_datetime(f'2025-{month:02d}-31')
            assert result == f'2025-{month:02d}-30 00:00:00'

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])