__author__ = 'xiajiguang1'

import pandas as pd
import sys

KEY_GROUP = ''


def main(sales_file, price_elasticity_file, top_saling_file):
    # read data
    sales_df = pd.read_table(sales_file)
    pe_df = pd.read_table(price_elasticity_file)
    top_saling_df = pd.read_table(top_saling_file)

    # join data
    df1 = pd.merge(sales_df, pe_df, on='item_sku_id', how='left')
    df1['pe'].fillna(value=0.0, inplace=True)
    df = pd.merge(df1, top_saling_df, how='left',
                  on=['item_third_cate_cd', 'item_sku_id', KEY_GROUP])
    df['true_top_saling_mark'].fillna(value=False, inplace=True)
    df['sales_share_recent_three_months'].fillna(value=0.0, inplace=True)
    df.rename(columns={KEY_GROUP: 'tm_key'}, inplace=True)

    # overall discount
    res = overall_discount(df)
    n = write_df(res)

    # top saling discount
    res = top_saling_discount(df)
    n = write_df(res, n)

    # long tail discount
    res = long_tail_discount(df)
    n = write_df(res, n)

    # high elasticity discount
    res = high_pe_discount(df)
    n = write_df(res, n)

    # low elasticity discount
    res = low_pe_discount(df)
    n = write_df(res, n)

    # add promotion flag
    add_promotion_flag(df)

    # on sale discount
    res = on_sale_discount(df)
    n = write_df(res, n)

    # off sale discount
    res = off_sale_discount(df)
    n = write_df(res, n)

    # add top saling discount when sku discount level > 0
    res = top_saling_on_sale_discount(df, 0)
    n = write_df(res, n)

    # add top saling gmv percentage when sku discount level > 0
    res = top_saling_on_sale_gmv_percent(df, 0)
    n = write_df(res, n)

    # add top saling discount when sku discount level > 0.05
    res = top_saling_on_sale_discount(df, 0.05)
    n = write_df(res, n)

    # add top saling gmv percentage discount level > 0
    res = top_saling_on_sale_gmv_percent(df, 0.05)
    n = write_df(res, n)

    # add top saling discount when discount level > 0.1
    res = top_saling_on_sale_discount(df, 0.1)
    n = write_df(res, n)

    # add top saling gmv percentage discount level > 0.1
    res = top_saling_on_sale_gmv_percent(df, 0.1)
    n = write_df(res, n)

    # add top saling discount when discount level > 0.2
    res = top_saling_on_sale_discount(df, 0.2)
    n = write_df(res, n)

    # add top saling gmv percentage discount level > 0.2
    res = top_saling_on_sale_gmv_percent(df, 0.2)
    n = write_df(res, n)

    # add top saling discount when sku discount level between 0 and 0.1
    res = top_saling_on_sale_discount(df, 0, 0.1)
    n = write_df(res, n)

    # add top saling gmv percentage sku discount level between 0 and 0.1
    res = top_saling_on_sale_gmv_percent(df, 0, 0.1)
    n = write_df(res, n)

    # add top saling discount when sku discount level between 0.1 and 0.2
    res = top_saling_on_sale_discount(df, 0.1, 0.2)
    n = write_df(res, n)

    # add top saling gmv percentage sku discount level between 0.1 and 0.2
    res = top_saling_on_sale_gmv_percent(df, 0.1, 0.2)
    n = write_df(res, n)




def write_df(df, n=0):
    fname = '../data/features_cg/f%d' % n
    df.to_csv(fname, sep='\t', header=True, index=False, quoting=None,
                 encoding='utf-8')
    return n+1


def overall_discount(df):
    df['overall_discount'] = df['sales_share_recent_three_months'] * \
                             df['total_synthetic_give_away'] / \
                             df['total_synthetic_gross_sales']
    df['overall_discount'].fillna(value=0.0, inplace=True)
    res = df.groupby(['tm_key','item_third_cate_cd']
                     )['overall_discount'].sum().reset_index()
    return res


def top_saling_discount(df):
    res = df[df['true_top_saling_mark']==True]\
        .groupby(['tm_key','item_third_cate_cd']
                 )[['total_synthetic_give_away','total_synthetic_gross_sales']]\
        .sum().reset_index()
    res['top_saling_discount'] = res['total_synthetic_give_away'] /\
                                 res['total_synthetic_gross_sales']
    return res[['tm_key', 'item_third_cate_cd', 'top_saling_discount']]


def long_tail_discount(df):
    res = df[df['true_top_saling_mark']==False] \
        .groupby(['tm_key','item_third_cate_cd']
                 )[['total_synthetic_give_away','total_synthetic_gross_sales']] \
        .sum().reset_index()
    res['long_tail_discount'] = res['total_synthetic_give_away'] / \
                                res['total_synthetic_gross_sales']
    return res[['tm_key', 'item_third_cate_cd', 'long_tail_discount']]


def high_pe_discount(df):
    res = df[df['pe'] < -12.5] \
        .groupby(['tm_key','item_third_cate_cd']
                 )[['total_synthetic_give_away','total_synthetic_gross_sales']] \
        .sum().reset_index()
    res['high_pe_discount'] = res['total_synthetic_give_away'] / \
                              res['total_synthetic_gross_sales']
    return res[['tm_key', 'item_third_cate_cd', 'high_pe_discount']]


def low_pe_discount(df):
    res = df[df['pe'] >= -12.5] \
        .groupby(['tm_key','item_third_cate_cd']
                 )[['total_synthetic_give_away','total_synthetic_gross_sales']] \
        .sum().reset_index()
    res['low_pe_discount'] = res['total_synthetic_give_away'] / \
                             res['total_synthetic_gross_sales']
    return res[['tm_key', 'item_third_cate_cd', 'low_pe_discount']]


def add_promotion_flag(df):
    df['discount'] = df['total_synthetic_give_away'] /\
                     df['total_synthetic_gross_sales']
    df['discount'].fillna(value=0.0, inplace=True)


def on_sale_discount(df):
    res = df[df['discount'] >= 0.05] \
        .groupby(['tm_key','item_third_cate_cd']
                 )[['total_synthetic_give_away','total_synthetic_gross_sales']] \
        .sum().reset_index()
    res['on_sale_discount'] = res['total_synthetic_give_away'] /\
                              res['total_synthetic_gross_sales']
    return res[['tm_key', 'item_third_cate_cd', 'on_sale_discount']]


def off_sale_discount(df):
    res = df[df['discount'] < 0.05] \
        .groupby(['tm_key','item_third_cate_cd']
                 )[['total_synthetic_give_away','total_synthetic_gross_sales']] \
        .sum().reset_index()
    res['off_sale_discount'] = res['total_synthetic_give_away'] / \
                               res['total_synthetic_gross_sales']
    return res[['tm_key', 'item_third_cate_cd', 'off_sale_discount']]


def top_saling_on_sale_discount(df, d1, d2=1):
    res = df[(df['true_top_saling_mark'] == True) &
             (df['discount'] > d1) &
             (df['discount'] <= d2)].groupby(
        ['tm_key', 'item_third_cate_cd'])[
        ['total_synthetic_give_away', 'total_synthetic_gross_sales']] \
        .sum().reset_index()
    col_name = 'top_saling_on_sale_discount_%d_to_%d' % (d1*100, d2*100)
    res[col_name] = res['total_synthetic_give_away'] /\
                    res['total_synthetic_gross_sales']
    res[col_name].fillna(value=0.0, inplace=True)
    return res[['tm_key', 'item_third_cate_cd', col_name]]


def top_saling_on_sale_gmv_percent(df, d1, d2=1):
    res_a = df.groupby(['tm_key', 'item_third_cate_cd']
                       )['total_net_sales'].sum()\
        .rename('total_gmv')
    res_b = df[(df['true_top_saling_mark'] == True) &
               (df['discount'] > d1) &
               (df['discount'] <= d2)].groupby(
        ['tm_key', 'item_third_cate_cd'])['total_net_sales'].sum()
    res = pd.DataFrame(res_a)
    res['total_net_sales'] = res_b
    col_name = 'top_saling_on_sale_gmv_percent_%d_to_%d' % (d1*100, d2*100)
    res[col_name] = res['total_net_sales'] / res['total_gmv']
    res[col_name].fillna(value=0.0, inplace=True)
    res = res.reset_index()
    return res[['tm_key', 'item_third_cate_cd', col_name]]


if __name__ == '__main__':
    # read command line arguments
    n = len(sys.argv) - 1
    if n != 4:
        print('Usage: \n    python discount_level.py sales_file price_elasticity_file top_saling_file mode\n')
        sys.exit()
    else:
        sales_file = sys.argv[1]
        price_elasticity_file = sys.argv[2]
        top_saling_file = sys.argv[3]
        mode = sys.argv[4]
        if mode == 'weekly':
            KEY_GROUP = 'sequentialweek'
        elif mode == 'monthly':
            KEY_GROUP = 'month'
        print("[INFO] discount level computation started ...")
        main(sales_file, price_elasticity_file, top_saling_file)
        print("[INFO] discount level computation done!")
