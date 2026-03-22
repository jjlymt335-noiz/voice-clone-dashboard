#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Voice Clone 数据看板 - 数据获取脚本
从 BigQuery 获取 Voice Clone 功能相关的 GA4 事件数据
"""

from google.cloud import bigquery
from datetime import datetime, timedelta
import json
import os

# 获取脚本所在目录的绝对路径
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

client = bigquery.Client(project='noiz-430406')

PROJECT_TABLE = 'noiz-430406.analytics_510746763'


def _combined_source(date_condition_daily, date_condition_intraday, extra_where=''):
    """生成 events_* + events_intraday_* 去重合并的 CTE SQL"""
    return f"""
    daily_dates AS (
        SELECT DISTINCT _TABLE_SUFFIX as dt
        FROM `{PROJECT_TABLE}.events_*`
        WHERE {date_condition_daily}
    ),
    combined_events AS (
        SELECT *
        FROM `{PROJECT_TABLE}.events_*`
        WHERE {date_condition_daily}
            {extra_where}
        UNION ALL
        SELECT *
        FROM `{PROJECT_TABLE}.events_intraday_*`
        WHERE {date_condition_intraday}
            AND _TABLE_SUFFIX NOT IN (SELECT dt FROM daily_dates)
            {extra_where}
    )
    """


def get_date_conditions(period):
    """根据周期返回 (daily条件, intraday条件)"""
    today = datetime.now()

    if period == '昨天':
        target_date = today - timedelta(days=1)
        suffix = target_date.strftime('%Y%m%d')
        daily = f"_TABLE_SUFFIX = '{suffix}'"
        intraday = f"_TABLE_SUFFIX BETWEEN '{suffix}' AND '{today.strftime('%Y%m%d')}'"
    elif period == '近3天':
        start_date = today - timedelta(days=3)
        daily = f"_TABLE_SUFFIX BETWEEN '{start_date.strftime('%Y%m%d')}' AND '{(today - timedelta(days=1)).strftime('%Y%m%d')}'"
        intraday = f"_TABLE_SUFFIX BETWEEN '{start_date.strftime('%Y%m%d')}' AND '{today.strftime('%Y%m%d')}'"
    else:  # 近7天
        start_date = today - timedelta(days=7)
        daily = f"_TABLE_SUFFIX BETWEEN '{start_date.strftime('%Y%m%d')}' AND '{(today - timedelta(days=1)).strftime('%Y%m%d')}'"
        intraday = f"_TABLE_SUFFIX BETWEEN '{start_date.strftime('%Y%m%d')}' AND '{today.strftime('%Y%m%d')}'"

    return daily, intraday


def get_funnel_data(period='近7天'):
    """获取核心漏斗数据"""
    daily_cond, intraday_cond = get_date_conditions(period)

    funnel_events = [
        'page_voice_clone_exposure',
        'voice_clone_add_voice_click',
        'voice_clone_preview_listen_play_click',
        'voice_clone_save_success',
    ]

    event_list = ','.join([f"'{e}'" for e in funnel_events])
    extra = f"AND event_name IN ({event_list})"

    query = f"""
    WITH {_combined_source(daily_cond, intraday_cond, extra)}
    SELECT
        event_name,
        COUNT(*) as event_count,
        COUNT(DISTINCT user_pseudo_id) as unique_users
    FROM combined_events
    GROUP BY event_name
    """

    results = client.query(query).result()
    data = {}
    for row in results:
        data[row.event_name] = {
            'count': row.event_count,
            'users': row.unique_users
        }

    # 离开页面
    exit_events = "('voice_clone_save_voice_use', 'voice_clone_complete_back')"
    extra_exit = f"AND event_name IN {exit_events}"

    query_exit = f"""
    WITH {_combined_source(daily_cond, intraday_cond, extra_exit)}
    SELECT
        event_name,
        COUNT(*) as event_count,
        COUNT(DISTINCT user_pseudo_id) as unique_users
    FROM combined_events
    GROUP BY event_name
    """
    for row in client.query(query_exit).result():
        data[row.event_name] = {
            'count': row.event_count,
            'users': row.unique_users
        }

    query_exit_total = f"""
    WITH {_combined_source(daily_cond, intraday_cond, extra_exit)}
    SELECT COUNT(DISTINCT user_pseudo_id) as unique_users,
           COUNT(*) as event_count
    FROM combined_events
    """
    exit_user_result = list(client.query(query_exit_total).result())[0]
    data['exit_page'] = {
        'count': exit_user_result.event_count,
        'users': exit_user_result.unique_users
    }

    return data


def get_step_details(period='近7天'):
    """获取步骤细分数据"""
    daily_cond, intraday_cond = get_date_conditions(period)
    details = {}

    # 步骤2：入口分布
    extra = "AND event_name IN ('creation_voice_clone_click', 'voice_library_voice_clone_click')"
    query_entry = f"""
    WITH {_combined_source(daily_cond, intraday_cond, extra)}
    SELECT event_name, COUNT(*) as event_count, COUNT(DISTINCT user_pseudo_id) as unique_users
    FROM combined_events
    GROUP BY event_name
    ORDER BY unique_users DESC
    """
    entry_data = {}
    for row in client.query(query_entry).result():
        entry_data[row.event_name] = {'count': row.event_count, 'users': row.unique_users}
    details['entry_distribution'] = entry_data

    # 步骤3：克隆声音添加 - 按来源分布
    extra = "AND event_name = 'voice_clone_add_voice_click'"
    query_add = f"""
    WITH {_combined_source(daily_cond, intraday_cond, extra)}
    SELECT
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'from') as from_path,
        COUNT(*) as event_count, COUNT(DISTINCT user_pseudo_id) as unique_users
    FROM combined_events
    GROUP BY from_path
    ORDER BY unique_users DESC
    LIMIT 5
    """
    add_voice_data = {}
    for row in client.query(query_add).result():
        add_voice_data[row.from_path or 'unknown'] = {'count': row.event_count, 'users': row.unique_users}
    details['add_voice_from'] = add_voice_data

    # 步骤4：手动选择片段
    extra = "AND event_name = 'voice_clone_select_manually'"
    query_manual = f"""
    WITH {_combined_source(daily_cond, intraday_cond, extra)}
    SELECT COUNT(*) as event_count, COUNT(DISTINCT user_pseudo_id) as unique_users
    FROM combined_events
    """
    manual_result = list(client.query(query_manual).result())[0]
    details['manual_select'] = {'count': manual_result.event_count, 'users': manual_result.unique_users}

    # 步骤5：保存音色时的描述修改
    extra = "AND event_name = 'voice_clone_save_description'"
    query_save_desc = f"""
    WITH {_combined_source(daily_cond, intraday_cond, extra)}
    SELECT
        COUNT(DISTINCT user_pseudo_id) as total_save_users,
        COUNT(DISTINCT CASE
            WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'description') !=
                 (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'original_description')
            THEN user_pseudo_id
        END) as with_desc_change
    FROM combined_events
    """
    save_desc_result = list(client.query(query_save_desc).result())[0]
    details['save_description'] = {
        'total_users': save_desc_result.total_save_users,
        'with_change': save_desc_result.with_desc_change
    }

    # 步骤6：离开页面分布
    extra = "AND event_name IN ('voice_clone_save_voice_use', 'voice_clone_complete_back')"
    query_exit = f"""
    WITH {_combined_source(daily_cond, intraday_cond, extra)}
    SELECT event_name, COUNT(*) as event_count, COUNT(DISTINCT user_pseudo_id) as unique_users
    FROM combined_events
    GROUP BY event_name
    """
    exit_data = {}
    for row in client.query(query_exit).result():
        exit_data[row.event_name] = {'count': row.event_count, 'users': row.unique_users}
    details['exit_distribution'] = exit_data

    return details


def get_deep_metrics(period='近7天'):
    """获取第二层深度指标"""
    daily_cond, intraday_cond = get_date_conditions(period)
    metrics = {}

    # 1. 完成率
    extra = "AND event_name IN ('page_voice_clone_exposure', 'voice_clone_save_success')"
    query_completion = f"""
    WITH {_combined_source(daily_cond, intraday_cond, extra)}
    SELECT
        COUNTIF(event_name = 'page_voice_clone_exposure') as exposure_count,
        COUNTIF(event_name = 'voice_clone_save_success') as save_count
    FROM combined_events
    """
    completion_result = list(client.query(query_completion).result())[0]
    exp_count = completion_result.exposure_count or 0
    save_count = completion_result.save_count or 0
    metrics['completion'] = {
        'exposure_count': exp_count,
        'save_count': save_count,
        'count_rate': round(save_count / (exp_count or 1) * 100, 2)
    }

    # 2. 保存后使用
    extra = "AND event_name IN ('voice_clone_save_success', 'voice_clone_save_voice_use')"
    query_tts = f"""
    WITH {_combined_source(daily_cond, intraday_cond, extra)}
    SELECT
        COUNTIF(event_name = 'voice_clone_save_success') as save_count,
        COUNT(DISTINCT IF(event_name = 'voice_clone_save_success', user_pseudo_id, NULL)) as save_users,
        COUNTIF(event_name = 'voice_clone_save_voice_use') as use_tts_count,
        COUNT(DISTINCT IF(event_name = 'voice_clone_save_voice_use', user_pseudo_id, NULL)) as use_tts_users
    FROM combined_events
    """
    tts_result = list(client.query(query_tts).result())[0]
    metrics['save_to_use'] = {
        'save_users': tts_result.save_users or 0,
        'save_count': tts_result.save_count or 0,
        'use_tts_users': tts_result.use_tts_users or 0,
        'use_tts_count': tts_result.use_tts_count or 0,
        'user_rate': round((tts_result.use_tts_users or 0) / (tts_result.save_users or 1) * 100, 2),
        'count_rate': round((tts_result.use_tts_count or 0) / (tts_result.save_count or 1) * 100, 2)
    }

    # 3. 付费转化
    extra = "AND event_name = 'voice_clone_upgrade_click'"
    query_upgrade = f"""
    WITH {_combined_source(daily_cond, intraday_cond, extra)}
    SELECT COUNT(*) as upgrade_click_count
    FROM combined_events
    """
    upgrade_result = list(client.query(query_upgrade).result())[0]
    metrics['upgrade_conversion'] = {
        'upgrade_click_count': upgrade_result.upgrade_click_count or 0
    }

    return metrics


def get_trend_data():
    """获取14天趋势数据"""
    today = datetime.now()
    start_date = today - timedelta(days=14)
    start_suffix = start_date.strftime('%Y%m%d')
    today_suffix = today.strftime('%Y%m%d')
    yesterday_suffix = (today - timedelta(days=1)).strftime('%Y%m%d')

    trend_events = """(
        'page_voice_clone_exposure',
        'voice_clone_add_voice_click',
        'voice_clone_preview_listen_play_click',
        'voice_clone_save_success'
    )"""

    query = f"""
    WITH daily_dates AS (
        SELECT DISTINCT _TABLE_SUFFIX as dt
        FROM `{PROJECT_TABLE}.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{start_suffix}' AND '{yesterday_suffix}'
    ),
    combined_events AS (
        SELECT event_date, event_name
        FROM `{PROJECT_TABLE}.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{start_suffix}' AND '{yesterday_suffix}'
            AND event_name IN {trend_events}
        UNION ALL
        SELECT event_date, event_name
        FROM `{PROJECT_TABLE}.events_intraday_*`
        WHERE _TABLE_SUFFIX BETWEEN '{start_suffix}' AND '{today_suffix}'
            AND event_name IN {trend_events}
            AND _TABLE_SUFFIX NOT IN (SELECT dt FROM daily_dates)
    )
    SELECT
        FORMAT_DATE('%m-%d', PARSE_DATE('%Y%m%d', event_date)) as date,
        event_name,
        COUNT(*) as event_count
    FROM combined_events
    GROUP BY date, event_name
    ORDER BY date
    """

    results = client.query(query).result()
    trend_data = {}
    for row in results:
        if row.date not in trend_data:
            trend_data[row.date] = {}
        trend_data[row.date][row.event_name] = row.event_count

    return trend_data


def main():
    print("开始获取 Voice Clone 数据看板数据...")

    all_data = {
        'funnel': {},
        'step_details': {},
        'deep_metrics': {},
        'trends': {},
        'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    periods = ['昨天', '近3天', '近7天']

    for period in periods:
        print(f"\n获取 {period} 数据...")

        print(f"  - 漏斗数据...")
        all_data['funnel'][period] = get_funnel_data(period)

        print(f"  - 步骤细分...")
        all_data['step_details'][period] = get_step_details(period)

        print(f"  - 深度指标...")
        all_data['deep_metrics'][period] = get_deep_metrics(period)

    print(f"\n获取 14天趋势数据...")
    all_data['trends'] = get_trend_data()

    output_path = os.path.join(SCRIPT_DIR, 'data', 'dashboard_data.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    print(f"\n数据已保存到 {output_path}")
    print("完成！")


if __name__ == '__main__':
    main()
