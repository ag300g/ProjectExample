__author__ = 'xiajiguang1'

import sys
import pandas as pd
import numpy as np

def top_saling(input_file, output_file, mode):
    if mode == 'weekly':
        top_saling_weekly(input_file, output_file)
    elif mode == 'monthly':
        top_saling_monthly(input_file, output_file)


def top_saling_weekly(input_file, output_file):
    '''
    :param input_file: must contains these columns
        item_third_cate_cd
        item_sku_id
        sequentialweek
        total_net_sales
    :param output_file: contains 4 columns
        item_third_cate_cd
        item_sku_id
        sequentialweek
        true_top_saling_mark
        sales_share_recent_three_months
    :return:
    '''

    df = pd.read_table(input_file)

    # set negative sales to zero
    df.loc[df['total_net_sales']<0, 'total_net_sales'] = 0

    '''
    计算近三个月(12周)以内的净销售额
    '''
    # sku level
    df['total_net_sales_recent_three_months'] = 0
    df1 = df[['item_sku_id', 'sequentialweek']].copy()
    for i in range(1, 13):
        df2 = df[['item_sku_id', 'sequentialweek', 'total_net_sales']].copy()
        df2['sequentialweek'] += i
        df3 = pd.merge(df1, df2, how='left',
                       on=['item_sku_id', 'sequentialweek'])
        df3['total_net_sales'].fillna(value=0, inplace=True)
        df['total_net_sales_recent_three_months'] += df3['total_net_sales']

    # 近三个月的净销售额汇总到三级品类
    df_cate3 = df.groupby(['sequentialweek','item_third_cate_cd'])\
        .agg({'total_net_sales_recent_three_months': sum})\
        .reset_index()\
        .rename(columns={'total_net_sales_recent_three_months':
                         'cate3_total_net_sales_recent_three_months'})

    '''
    计算每周的top saling skus
    '''
    # 将dataframe按照近三个月销售额倒序排序
    df_sorted = df[['sequentialweek', 'item_third_cate_cd', 'item_sku_id',
                    'total_net_sales_recent_three_months']]\
        .sort_values(by=['total_net_sales_recent_three_months'],
                     ascending=False)
    # 按照周和三级分类分组，组内对SKU求累计销量
    df_sorted['cumulated_net_sales'] = df_sorted \
        .groupby(['sequentialweek','item_third_cate_cd']
                 )['total_net_sales_recent_three_months']\
        .transform(np.cumsum)
    # 合并累积销量
    df_sku_cate3 = pd.merge(df_sorted, df_cate3,
                            on=['sequentialweek', 'item_third_cate_cd'],
                            how='inner')
    # 在三级分类内计算当前sku的比例
    df_sku_cate3['sales_share_recent_three_months'] =\
        df_sku_cate3['total_net_sales_recent_three_months'] /\
        df_sku_cate3['cate3_total_net_sales_recent_three_months']
    df_sku_cate3['sales_share_recent_three_months']\
        .fillna(value=0.0, inplace=True)
    # 在三级分类内计算top1至当前sku的累积比例
    df_sku_cate3['cumulated_percentagecase'] =\
        df_sku_cate3['cumulated_net_sales'] /\
        df_sku_cate3['cate3_total_net_sales_recent_three_months']
    df_sku_cate3['cumulated_percentagecase'].fillna(value=0.0, inplace=True)
    # 在三级分类内计算top1至前一个sku的累积比例
    df_sku_cate3['last_cumulated_percentage'] =\
        df_sku_cate3['cumulated_percentagecase'] -\
        df_sku_cate3['sales_share_recent_three_months']
    # 标记热销品
    df_sku_cate3['true_top_saling_mark'] =\
        (df_sku_cate3['cumulated_percentagecase'] >= 0.8) &\
        (df_sku_cate3['last_cumulated_percentage'] < 0.8) |\
        (df_sku_cate3['cumulated_percentagecase'] > 0) &\
        (df_sku_cate3['cumulated_percentagecase'] < 0.8)

    '''
    save result
    '''
    df_sku_cate3[['item_third_cate_cd', 'item_sku_id', 'sequentialweek',
                  'true_top_saling_mark', 'sales_share_recent_three_months']]\
        .to_csv(output_file, sep='\t', header=True, index=False, quoting=None,
                encoding='utf-8')


def top_saling_monthly(input_file, output_file):
    '''
    :param input_file: must contains these columns
        item_third_cate_cd
        item_sku_id
        month
        total_net_sales
    :param output_file: contains 4 columns
        item_third_cate_cd
        item_sku_id
        month
        true_top_saling_mark
        sales_share_recent_three_months
    :return:
    '''

    df = pd.read_table(input_file)

    # set negative sales to zero
    df.loc[df['total_net_sales']<0, 'total_net_sales'] = 0

    # add month sequence
    years = df['month'].apply(lambda x: pd.to_datetime(x).year)
    months = df['month'].apply(lambda x: pd.to_datetime(x).month)
    min_year = min(years)
    df['sequentialmonth'] = (years - min_year) * 12 + months

    '''
    计算近三个月以内的净销售额
    '''
    # sku level
    df['total_net_sales_recent_three_months'] = 0
    df1 = df[['item_sku_id', 'sequentialmonth']].copy()
    for i in range(1, 4):
        df2 = df[['item_sku_id', 'sequentialmonth', 'total_net_sales']].copy()
        df2['sequentialmonth'] += i
        df3 = pd.merge(df1, df2, how='left',
                       on=['item_sku_id', 'sequentialmonth'])
        df3['total_net_sales'].fillna(value=0, inplace=True)
        df['total_net_sales_recent_three_months'] += df3['total_net_sales']

    # 近三个月的净销售额汇总到三级品类
    df_cate3 = df.groupby(['month','item_third_cate_cd']) \
        .agg({'total_net_sales_recent_three_months': sum}) \
        .reset_index() \
        .rename(columns={'total_net_sales_recent_three_months':
                             'cate3_total_net_sales_recent_three_months'})

    '''
    计算每月的top saling skus
    '''
    # 将dataframe按照近三个月销售额倒序排序
    df_sorted = df[['month', 'item_third_cate_cd', 'item_sku_id',
                    'total_net_sales_recent_three_months']] \
        .sort_values(by=['total_net_sales_recent_three_months'],
                     ascending=False)
    # 按照周和三级分类分组，组内对SKU求累计销量
    df_sorted['cumulated_net_sales'] = df_sorted \
        .groupby(['month','item_third_cate_cd']
                 )['total_net_sales_recent_three_months'] \
        .transform(np.cumsum)
    # 合并累积销量
    df_sku_cate3 = pd.merge(df_sorted, df_cate3,
                            on=['month', 'item_third_cate_cd'],
                            how='inner')
    # 在三级分类内计算当前sku的比例
    df_sku_cate3['sales_share_recent_three_months'] = \
        df_sku_cate3['total_net_sales_recent_three_months'] / \
        df_sku_cate3['cate3_total_net_sales_recent_three_months']
    df_sku_cate3['sales_share_recent_three_months'] \
        .fillna(value=0.0, inplace=True)
    # 在三级分类内计算top1至当前sku的累积比例
    df_sku_cate3['cumulated_percentagecase'] = \
        df_sku_cate3['cumulated_net_sales'] / \
        df_sku_cate3['cate3_total_net_sales_recent_three_months']
    df_sku_cate3['cumulated_percentagecase'].fillna(value=0.0, inplace=True)
    # 在三级分类内计算top1至前一个sku的累积比例
    df_sku_cate3['last_cumulated_percentage'] = \
        df_sku_cate3['cumulated_percentagecase'] - \
        df_sku_cate3['sales_share_recent_three_months']
    # 标记热销品
    df_sku_cate3['true_top_saling_mark'] = \
        (df_sku_cate3['cumulated_percentagecase'] >= 0.8) & \
        (df_sku_cate3['last_cumulated_percentage'] < 0.8) | \
        (df_sku_cate3['cumulated_percentagecase'] > 0) & \
        (df_sku_cate3['cumulated_percentagecase'] < 0.8)

    '''
    save result
    '''
    df_sku_cate3[['item_third_cate_cd', 'item_sku_id', 'month',
                  'true_top_saling_mark', 'sales_share_recent_three_months']] \
        .to_csv(output_file, sep='\t', header=True, index=False, quoting=None,
                encoding='utf-8')



if __name__ == '__main__':
    # read command line arguments
    n = len(sys.argv) - 1
    if n != 3:
        print('Usage: \n    python top_saling.py input_file output_file mode\n')
        sys.exit()
    else:
        input_file = sys.argv[1]
        output_file = sys.argv[2]
        mode = sys.argv[3]
        print("[INFO] top_saling computation started ...")
        top_saling(input_file, output_file, mode)
        print("[INFO] top_saling computation done!")
