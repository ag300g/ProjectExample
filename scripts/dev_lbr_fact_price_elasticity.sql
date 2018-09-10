DROP TABLE IF EXISTS dev.dev_lbr_fact_price_elasticity;
CREATE TABLE dev.dev_lbr_fact_price_elasticity
AS
SELECT
    a.skuid as item_sku_id,
    COALESCE(a.e_value, b.e_value, c.e_value) AS pe
FROM
    app.app_cis_dps3_final_sku_result a
LEFT JOIN
    app.app_cis_dps3_final_brand_result b
ON
    a.brand_code = b.brand_code
AND
    a.item_third_cate_cd = b.item_third_cate_cd
LEFT JOIN
    app.app_cis_dps3_final_cate_result c
ON
    a.item_third_cate_cd = c.item_third_cate_cd
WHERE
    a.dt = '2017-06-17'
AND
    b.dt='2017-06-17'
AND
    c.dt='2017-06-17';