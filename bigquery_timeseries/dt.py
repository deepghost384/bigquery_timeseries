# dt.py

import pandas as pd
from datetime import datetime


def is_date(dt: str) -> bool:
    try:
        datetime.strptime(dt, "%Y-%m-%d")
    except ValueError:
        return False
    return True


def to_quarter(month: int) -> int:
    return (month - 1) // 3 + 1


def to_month_start_dt(dt: pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(year=dt.year, month=dt.month, day=1)


def to_month_end_dt(dt: pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(year=dt.year, month=dt.month, day=1) + pd.offsets.MonthEnd()


def to_quarter_start_dt(dt: pd.Timestamp, offset: int = 0) -> pd.Timestamp:
    """Compute beginning date of quarter from given timestamp"""
    quarter = to_quarter(dt.month)

    quarter_start_month = (quarter - 1) * 3 + 1

    dt = pd.Timestamp(year=dt.year, month=quarter_start_month, day=1)

    if offset > 0:
        dt = dt - pd.offsets.MonthBegin(3 * offset)

    return dt


def to_quarter_end_dt(dt: pd.Timestamp, offset: int = 0) -> pd.Timestamp:
    return to_quarter_start_dt(dt, offset) + pd.offsets.MonthEnd(3)


def compute_intervals(
    start_dt: str,
    end_dt: str,
    days: int = 5,
    fmt: str = "%Y-%m-%d",
    offset=pd.offsets.Day(),
):
    _start_dt = pd.Timestamp(start_dt)
    _end_dt = pd.Timestamp(end_dt)

    while True:
        current_end_dt = (_start_dt + pd.offsets.Day(days)).normalize()

        if current_end_dt >= _end_dt:
            yield _start_dt.strftime(fmt), _end_dt.strftime(fmt)
            return

        yield _start_dt.strftime(fmt), current_end_dt.strftime(fmt)

        _start_dt = current_end_dt + offset


def compute_monthly_intervals(start_dt: str, end_dt: str):
    fmt = "%Y-%m-%d"
    datetime.strptime(start_dt, fmt)
    datetime.strptime(end_dt, fmt)

    _start_dt = pd.Timestamp(start_dt)
    _end_dt = pd.Timestamp(end_dt)

    while True:
        current_end_dt = _start_dt.replace(day=1) + pd.offsets.MonthEnd()

        if current_end_dt >= _end_dt:
            yield _start_dt.strftime(fmt), _end_dt.strftime(fmt)
            return

        yield _start_dt.strftime(fmt), current_end_dt.strftime(fmt)

        _start_dt = current_end_dt + pd.offsets.Day()

def normalize_datetime(dt_str: str) -> str:
    """
    日付文字列を正規化し、存在しない日付を自動的に修正する
    
    例:
    - '2025-09-31' -> '2025-09-30'（9月は30日まで）
    - '2025-02-30' -> '2025-02-28'（平年の2月は28日まで）
    - '2024-02-30' -> '2024-02-29'（閏年の2月は29日まで）
    
    Args:
        dt_str: 日付文字列（'YYYY-MM-DD' または 'YYYY-MM-DD HH:MM:SS'）
    
    Returns:
        正規化された日付文字列
    """
    try:
        # まず通常の変換を試みる
        ts = pd.Timestamp(dt_str)
        return ts.strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, pd.errors.OutOfBoundsDatetime):
        # 失敗した場合、日付部分を解析して修正
        try:
            # 時刻部分があるかチェック
            if ' ' in dt_str:
                date_part, time_part = dt_str.split(' ', 1)
            else:
                date_part = dt_str
                time_part = '00:00:00'
            
            # 日付部分を分解
            parts = date_part.split('-')
            if len(parts) != 3:
                raise ValueError(f"Invalid date format: {dt_str}")
            
            year = int(parts[0])
            month = int(parts[1])
            day = int(parts[2])
            
            # 月が有効な範囲にあるか確認
            if month < 1 or month > 12:
                raise ValueError(f"Invalid month: {month}")
            
            # その月の最終日を取得
            # 月の1日を作成し、次の月の1日から1日引く
            first_day_of_next_month = pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(0)
            max_day = first_day_of_next_month.day
            
            # 日が有効な範囲を超えている場合は最終日に修正
            if day > max_day:
                day = max_day
            
            # 正規化された日付を作成
            normalized_ts = pd.Timestamp(year=year, month=month, day=day)
            
            # 時刻部分を追加
            time_parts = time_part.split(':')
            if len(time_parts) >= 2:
                hour = int(time_parts[0])
                minute = int(time_parts[1])
                second = int(time_parts[2]) if len(time_parts) >= 3 else 0
                
                normalized_ts = normalized_ts.replace(hour=hour, minute=minute, second=second)
            
            return normalized_ts.strftime('%Y-%m-%d %H:%M:%S')
            
        except Exception as e:
            raise ValueError(f"Cannot normalize datetime string '{dt_str}': {e}")