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

def get_date_condition(period):
    """根据周期返回日期条件"""
    today = datetime.now()

    if period == '昨天':
        target_date = today - timedelta(days=1)
        return f"_TABLE_SUFFIX = '{target_date.strftime('%Y%m%d')}'"
    elif period == '近3天':
        end_date = today - timedelta(days=1)
        start_date = today - timedelta(days=3)
        return f"_TABLE_SUFFIX BETWEEN '{start_date.strftime('%Y%m%d')}' AND '{end_date.strftime('%Y%m%d')}'"
    else:  # 近7天
        end_date = today - timedelta(days=1)
        start_date = today - timedelta(days=7)
        return f"_TABLE_SUFFIX BETWEEN '{start_date.strftime('%Y%m%d')}' AND '{end_date.strftime('%Y%m%d')}'"

def get_funnel_data(period='近7天'):
    """获取核心漏斗数据"""
    date_condition = get_date_condition(period)

    # 漏斗事件
    funnel_events = [
        'page_voice_clone_exposure',           # 1. 曝光（注意：正确拼写是 exposure）
        'voice_clone_add_voice_click',         # 3. 克隆声音添加
        'voice_clone_preview_listen_play_click', # 4. 试听
        'voice_clone_save_success',            # 5. 保存成功
    ]

    query = f"""
    SELECT
        event_name,
        COUNT(*) as event_count,
        COUNT(DISTINCT user_pseudo_id) as unique_users
    FROM `noiz-430406.analytics_510746763.events_intraday_*`
    WHERE {date_condition}
        AND event_name IN UNNEST(@events)
    GROUP BY event_name
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("events", "STRING", funnel_events)
        ]
    )

    results = client.query(query, job_config=job_config).result()

    data = {}
    for row in results:
        data[row.event_name] = {
            'count': row.event_count,
            'users': row.unique_users
        }

    # 计算步骤6：离开页面（use + back）
    query_exit = f"""
    SELECT
        event_name,
        COUNT(*) as event_count,
        COUNT(DISTINCT user_pseudo_id) as unique_users
    FROM `noiz-430406.analytics_510746763.events_intraday_*`
    WHERE {date_condition}
        AND event_name IN ('voice_clone_save_voice_use', 'voice_clone_complete_back')
    GROUP BY event_name
    """

    exit_results = client.query(query_exit).result()
    exit_users = set()
    exit_count = 0

    for row in exit_results:
        data[row.event_name] = {
            'count': row.event_count,
            'users': row.unique_users
        }
        exit_count += row.event_count

    # 获取离开页面的去重用户数
    query_exit_users = f"""
    SELECT COUNT(DISTINCT user_pseudo_id) as unique_users,
           COUNT(*) as event_count
    FROM `noiz-430406.analytics_510746763.events_intraday_*`
    WHERE {date_condition}
        AND event_name IN ('voice_clone_save_voice_use', 'voice_clone_complete_back')
    """
    exit_user_result = list(client.query(query_exit_users).result())[0]
    data['exit_page'] = {
        'count': exit_user_result.event_count,
        'users': exit_user_result.unique_users
    }

    return data

def get_step_details(period='近7天'):
    """获取步骤细分数据"""
    date_condition = get_date_condition(period)
    details = {}

    # 步骤2：入口分布 - 从哪里进入克隆页面
    query_entry = f"""
    SELECT
        event_name,
        COUNT(*) as event_count,
        COUNT(DISTINCT user_pseudo_id) as unique_users
    FROM `noiz-430406.analytics_510746763.events_intraday_*`
    WHERE {date_condition}
        AND event_name IN ('creation_voice_clone_click', 'voice_library_voice_clone_click')
    GROUP BY event_name
    ORDER BY unique_users DESC
    """

    entry_results = client.query(query_entry).result()
    entry_data = {}
    for row in entry_results:
        entry_data[row.event_name] = {
            'count': row.event_count,
            'users': row.unique_users
        }
    details['entry_distribution'] = entry_data

    # 步骤3：克隆声音添加 - 按来源分布（from 参数）
    query_add_voice = f"""
    SELECT
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'from') as from_path,
        COUNT(*) as event_count,
        COUNT(DISTINCT user_pseudo_id) as unique_users
    FROM `noiz-430406.analytics_510746763.events_intraday_*`
    WHERE {date_condition}
        AND event_name = 'voice_clone_add_voice_click'
    GROUP BY from_path
    ORDER BY unique_users DESC
    LIMIT 5
    """

    add_results = client.query(query_add_voice).result()
    add_voice_data = {}
    for row in add_results:
        from_path = row.from_path or 'unknown'
        add_voice_data[from_path] = {
            'count': row.event_count,
            'users': row.unique_users
        }
    details['add_voice_from'] = add_voice_data

    # 步骤4：手动选择片段
    query_manual = f"""
    SELECT
        COUNT(*) as event_count,
        COUNT(DISTINCT user_pseudo_id) as unique_users
    FROM `noiz-430406.analytics_510746763.events_intraday_*`
    WHERE {date_condition}
        AND event_name = 'voice_clone_select_manually'
    """

    manual_result = list(client.query(query_manual).result())[0]
    details['manual_select'] = {
        'count': manual_result.event_count,
        'users': manual_result.unique_users
    }

    # 步骤5：保存音色时的描述修改
    query_save_desc = f"""
    SELECT
        COUNT(DISTINCT user_pseudo_id) as total_save_users,
        COUNT(DISTINCT CASE
            WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'description') !=
                 (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'original_description')
            THEN user_pseudo_id
        END) as with_desc_change
    FROM `noiz-430406.analytics_510746763.events_intraday_*`
    WHERE {date_condition}
        AND event_name = 'voice_clone_save_description'
    """

    save_desc_result = list(client.query(query_save_desc).result())[0]
    details['save_description'] = {
        'total_users': save_desc_result.total_save_users,
        'with_change': save_desc_result.with_desc_change
    }

    # 步骤6：离开页面分布
    query_exit = f"""
    SELECT
        event_name,
        COUNT(*) as event_count,
        COUNT(DISTINCT user_pseudo_id) as unique_users
    FROM `noiz-430406.analytics_510746763.events_intraday_*`
    WHERE {date_condition}
        AND event_name IN ('voice_clone_save_voice_use', 'voice_clone_complete_back')
    GROUP BY event_name
    """

    exit_results = client.query(query_exit).result()
    exit_data = {}
    for row in exit_results:
        exit_data[row.event_name] = {
            'count': row.event_count,
            'users': row.unique_users
        }
    details['exit_distribution'] = exit_data

    return details

def get_deep_metrics(period='近7天'):
    """获取第二层深度指标"""
    date_condition = get_date_condition(period)
    metrics = {}

    # 1. 完成率 = 保存音色用户 / 曝光用户
    query_completion = f"""
    WITH exposure_users AS (
        SELECT COUNT(DISTINCT user_pseudo_id) as users
        FROM `noiz-430406.analytics_510746763.events_intraday_*`
        WHERE {date_condition}
            AND event_name = 'page_voice_clone_exposure'
    ),
    save_users AS (
        SELECT COUNT(DISTINCT user_pseudo_id) as users
        FROM `noiz-430406.analytics_510746763.events_intraday_*`
        WHERE {date_condition}
            AND event_name = 'voice_clone_save_success'
    )
    SELECT
        (SELECT users FROM exposure_users) as exposure_users,
        (SELECT users FROM save_users) as save_users
    """

    completion_result = list(client.query(query_completion).result())[0]
    metrics['completion'] = {
        'exposure_users': completion_result.exposure_users or 0,
        'save_users': completion_result.save_users or 0,
        'rate': round((completion_result.save_users or 0) / (completion_result.exposure_users or 1) * 100, 2)
    }

    # 2. 保存后使用：保存音色后去TTS使用的用户占比
    query_tts_clone = f"""
    WITH save_users AS (
        SELECT COUNT(DISTINCT user_pseudo_id) as users,
               COUNT(*) as count
        FROM `noiz-430406.analytics_510746763.events_intraday_*`
        WHERE {date_condition}
            AND event_name = 'voice_clone_save_success'
    ),
    use_tts_users AS (
        SELECT COUNT(DISTINCT user_pseudo_id) as users,
               COUNT(*) as count
        FROM `noiz-430406.analytics_510746763.events_intraday_*`
        WHERE {date_condition}
            AND event_name = 'voice_clone_save_voice_use'
    )
    SELECT
        (SELECT users FROM save_users) as save_users,
        (SELECT count FROM save_users) as save_count,
        (SELECT users FROM use_tts_users) as use_tts_users,
        (SELECT count FROM use_tts_users) as use_tts_count
    """

    tts_result = list(client.query(query_tts_clone).result())[0]
    metrics['save_to_use'] = {
        'save_users': tts_result.save_users or 0,
        'save_count': tts_result.save_count or 0,
        'use_tts_users': tts_result.use_tts_users or 0,
        'use_tts_count': tts_result.use_tts_count or 0,
        'user_rate': round((tts_result.use_tts_users or 0) / (tts_result.save_users or 1) * 100, 2),
        'count_rate': round((tts_result.use_tts_count or 0) / (tts_result.save_count or 1) * 100, 2)
    }

    # 3. 付费转化：upgrade_click 并付费
    query_upgrade = f"""
    WITH upgrade_click_users AS (
        SELECT DISTINCT user_pseudo_id
        FROM `noiz-430406.analytics_510746763.events_intraday_*`
        WHERE {date_condition}
            AND event_name = 'voice_clone_upgrade_click'
    ),
    paid_users AS (
        SELECT DISTINCT user_pseudo_id
        FROM `noiz-430406.analytics_510746763.events_intraday_*`
        WHERE {date_condition}
            AND event_name IN ('purchase', 'in_app_purchase', 'subscription_purchase')
    )
    SELECT
        (SELECT COUNT(*) FROM upgrade_click_users) as upgrade_click_users,
        COUNT(*) as upgrade_and_paid_users
    FROM upgrade_click_users u
    JOIN paid_users p ON u.user_pseudo_id = p.user_pseudo_id
    """

    upgrade_result = list(client.query(query_upgrade).result())[0]
    metrics['upgrade_conversion'] = {
        'upgrade_click_users': upgrade_result.upgrade_click_users or 0,
        'upgrade_and_paid_users': upgrade_result.upgrade_and_paid_users or 0,
        'rate': round((upgrade_result.upgrade_and_paid_users or 0) / (upgrade_result.upgrade_click_users or 1) * 100, 2)
    }

    return metrics

def get_trend_data(period='近7天'):
    """获取趋势数据"""
    today = datetime.now()

    if period == '昨天':
        days = 1
    elif period == '近3天':
        days = 3
    else:
        days = 7

    end_date = today - timedelta(days=1)
    start_date = today - timedelta(days=days)
    date_condition = f"_TABLE_SUFFIX BETWEEN '{start_date.strftime('%Y%m%d')}' AND '{end_date.strftime('%Y%m%d')}'"

    query = f"""
    SELECT
        FORMAT_DATE('%m-%d', PARSE_DATE('%Y%m%d', event_date)) as date,
        event_name,
        COUNT(DISTINCT user_pseudo_id) as unique_users
    FROM `noiz-430406.analytics_510746763.events_intraday_*`
    WHERE {date_condition}
        AND event_name IN (
            'page_voice_clone_exposure',
            'voice_clone_add_voice_click',
            'voice_clone_preview_listen_play_click',
            'voice_clone_save_success'
        )
    GROUP BY date, event_name
    ORDER BY date
    """

    results = client.query(query).result()

    trend_data = {}
    for row in results:
        if row.date not in trend_data:
            trend_data[row.date] = {}
        trend_data[row.date][row.event_name] = row.unique_users

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

        print(f"  - 趋势数据...")
        all_data['trends'][period] = get_trend_data(period)

    # 保存数据
    output_path = os.path.join(SCRIPT_DIR, 'data', 'dashboard_data.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    print(f"\n数据已保存到 {output_path}")
    print("完成！")

if __name__ == '__main__':
    main()
