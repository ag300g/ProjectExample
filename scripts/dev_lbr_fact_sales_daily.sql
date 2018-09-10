DROP TABLE IF EXISTS dev.dev_lbr_fact_sales_daily_00;
CREATE TABLE dev.dev_lbr_fact_sales_daily_00
AS
SELECT
    sale_ord_dt,
    item_sku_id,
    sum(after_prefr_amount) AS net_sales,
    sum(before_prefr_amount) AS gross_sales,
    sum(sale_qtty) AS unit_volume,
    sum(after_prefr_amount)/sum(sale_qtty) AS net_price,
    sum(before_prefr_amount)/sum(sale_qtty) AS gross_price,
FROM
    gdm.gdm_m04_ord_det_sum
WHERE
    dp = 'HISTORY'
AND
    dt >= '2014-07-01'
AND
    sale_ord_dt >= '2014-07-01'
AND
    sale_ord_valid_flag = 1
GROUP BY
    sale_ord_dt,
    item_sku_id
;

-- add synthetic_gross_price
DROP TABLE IF EXISTS dev.dev_lbr_fact_synthetic_gross_price;
CREATE TABLE dev.dev_lbr_fact_synthetic_gross_price
AS
SELECT
    date,
    productkey as item_sku_id,
    syntheticgrossprice as synthetic_gross_price
FROM
    dev.owforecast_sales_dayproduct_syntheticpromoflag_excloutstandingprices_new;


DROP TABLE IF EXISTS dev.dev_lbr_fact_sales_daily_01;
CREATE TABLE dev.dev_lbr_fact_sales_daily_01
AS
SELECT
    s.*,
    p.synthetic_gross_price
FROM
    dev.dev_lbr_fact_sales_daily_00 s
LEFT JOIN
    dev.dev_lbr_fact_synthetic_gross_price p
ON
    p.item_sku_id = s.item_sku_id
    AND
    p.date = s.sale_ord_dt
;


-- add category id
DROP TABLE IF EXISTS dev.dev_lbr_fact_sales_daily_02;
CREATE TABLE dev.dev_lbr_fact_sales_daily_02
AS
SELECT
    s.*,
    p.item_first_cate_cd,
    p.item_second_cate_cd,
    p.item_third_cate_cd
FROM
    dev.dev_lbr_fact_sales_daily_01 s
INNER JOIN
    (select * from gdm.gdm_m03_self_item_sku_da
    WHERE
        dt = '2017-04-28'
    ) p
ON
    p.item_sku_id = s.item_sku_id
;

-- add week and month
DROP TABLE IF EXISTS dev.dev_lbr_fact_sales_daily_03;
CREATE TABLE dev.dev_lbr_fact_sales_daily_03
AS
SELECT
    d.date,
    item_first_cate_cd,
    item_second_cate_cd,
    item_third_cate_cd,
    item_sku_id,
    unit_volume,
    net_sales as total_net_sales,
    gross_sales as total_gross_sales,
    unit_volume * synthetic_gross_price as total_synthetic_gross_sales,
    gross_sales - net_sales as total_give_away,
    if(synthetic_gross_price=0, 0, unit_volume * (synthetic_gross_price - net_price)) as total_synthetic_give_away,
    (gross_sales - net_sales) / gross_sales as raw_discount_percentage,
    (synthetic_gross_price - net_price) / synthetic_gross_price as synthetic_discount_percentage,
    d.sequentialweek,
    w.date as week_start,
    SUBSTRING(s.sale_ord_dt, 1, 7) as month
FROM
    dev.dev_lbr_fact_sales_daily_02 s
INNER JOIN
    dev.dev_lbr_dim_date d
ON
    s.sale_ord_dt = d.date
LEFT JOIN
    dev.dev_lbr_dim_week w
ON
    d.sequentialweek = w.sequentialweek
;


-- aggregate by sku and week
DROP TABLE IF EXISTS dev.dev_lbr_fact_sales_weekly;
CREATE TABLE dev.dev_lbr_fact_sales_weekly
AS
SELECT
    item_first_cate_cd,
    item_second_cate_cd,
    item_third_cate_cd,
    item_sku_id,
    sequentialweek,
    week_start,
    sum(total_net_sales) as total_net_sales,
    sum(total_give_away) as total_give_away,
    sum(total_gross_sales) as total_gross_sales,
    sum(total_synthetic_give_away) as total_synthetic_give_away,
    sum(total_synthetic_gross_sales) as total_synthetic_gross_sales
FROM
    dev.dev_lbr_fact_sales_daily_03 s
GROUP BY
    item_first_cate_cd,
    item_second_cate_cd,
    item_third_cate_cd,
    item_sku_id,
    sequentialweek,
    week_start
;


-- aggregate by sku and month
DROP TABLE IF EXISTS dev.dev_lbr_fact_sales_monthly;
CREATE TABLE dev.dev_lbr_fact_sales_monthly
AS
SELECT
    item_first_cate_cd,
    item_second_cate_cd,
    item_third_cate_cd,
    item_sku_id,
    month,
    sum(total_net_sales) as total_net_sales,
    sum(total_give_away) as total_give_away,
    sum(total_gross_sales) as total_gross_sales,
    sum(total_synthetic_give_away) as total_synthetic_give_away,
    sum(total_synthetic_gross_sales) as total_synthetic_gross_sales
FROM
    dev.dev_lbr_fact_sales_daily_03 s
GROUP BY
    item_first_cate_cd,
    item_second_cate_cd,
    item_third_cate_cd,
    item_sku_id,
    month
;
