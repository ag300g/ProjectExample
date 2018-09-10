#!/usr/bin/env bash

hive -f dev_lbr_fact_price_elasticity.sql
hive -f dev_lbr_fact_sales_daily.sql
hive -e "set hive.cli.print.header=true; select * from dev.dev_lbr_fact_price_elasticity;" > ../data/dev_lbr_fact_price_elasticity
hive -e "set hive.cli.print.header=true; select * from dev.dev_lbr_fact_sales_monthly where item_third_cate_cd=672 or item_first_cate_cd in (1320, 9987, 1319) or item_second_cate_cd=1343;" > ../data/dev_lbr_fact_sales_monthly
python top_saling.py ../data/dev_lbr_fact_sales_monthly ../data/dev_lbr_fact_top_saling_monthly monthly
python discount_level.py ../data/dev_lbr_fact_sales_monthly ../data/dev_lbr_fact_price_elasticity ../data/dev_lbr_fact_top_saling_monthly monthly

