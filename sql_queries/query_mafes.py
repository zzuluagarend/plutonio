def query_mafe_experiments(country, region_code, warehouse, today, days, dataset_id):
    
    query = f"""
        WITH
        ids AS 
        (
            SELECT  MAX(dpf.id) AS dp_forecast_params_id,
                    dp_input_datasets_id,
                    region_code,
                    warehouse
            FROM postgres_main_federate."ops_demand_planning.forecast_context.dp_forecast_params_identifiers" dpf
            LEFT JOIN postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" dpi
                ON dpf.dp_input_datasets_id = dpi.id
            WHERE   dp_input_datasets_id = {dataset_id}
            GROUP BY dp_input_datasets_id,
                    region_code,
                    warehouse
            ORDER BY 1
        ),
        
        t_warehouses AS
        (
            SELECT      dw.identifier_value AS warehouse
            ,           is_3pl
            FROM        dpr_shared.dim_site ds
            LEFT JOIN   dpr_shared.dim_warehouse dw ON ds.site_id = dw.site_id
            WHERE       ds.identifier_value NOT IN ('NaN', 'FBO' , 'FBA')
                        AND ds.identifier_value = '{ region_code }'
                        AND dw.identifier_value NOT IN ('CO', 'CEASABOX1', 'SALR')
        ),

        t_products AS
        (
            SELECT      COALESCE(S.new_product_id, orders_products.product_id) AS product_id
            ,           MIN(CASE WHEN owner_id IN (1, 5354, 8354, 6104, 7404) THEN '1pl' ELSE '3pl' END) AS warehouse_type
            FROM        postgres_main_{ country }."reports.orders" orders
            LEFT JOIN   postgres_main_{ country }."reports.orders_products" orders_products ON orders.id = orders_products.order_id
            LEFT JOIN   postgres_main_{ country }."reports.sandbox.new_skus" S ON orders_products.product_id = S.old_product_id
            WHERE       status = 1
                        AND is_deleted = 'false'
                        AND close_date >= DATE('{ today }') - INTERVAL '30 days'
                        AND region_code = '{ region_code }'
            GROUP BY    COALESCE(S.new_product_id, orders_products.product_id)
            HAVING      CASE  WHEN '{ warehouse }' IN (SELECT warehouse FROM t_warehouses WHERE is_3pl = 'True') THEN warehouse_type <> '1pl' 
                              WHEN '{ warehouse }' IN (SELECT warehouse FROM t_warehouses WHERE is_3pl = 'False') THEN warehouse_type = '1pl' 
                              ELSE 1=1 
                        END 
        ),

        t_clean_demand AS
        (
            SELECT      sales.date AS close_date,
                        ids.region_code,
                        COALESCE(ns.new_product_id, sales.product_id) AS product_id,
                        SUM(sales.demand * COALESCE(ns.old_units_per_new_unit, 1)) AS demand
            FROM        lnd_ops_dev.dp_plutonio sales
            LEFT JOIN   postgres_main_{ country }."reports.sandbox.new_skus" ns ON sales.product_id = ns.old_product_id
            INNER JOIN  ids ON sales.dp_input_datasets_id = ids.dp_input_datasets_id
            INNER JOIN  t_products toi ON COALESCE(ns.new_product_id, sales.product_id) = toi.product_id
            WHERE       sales.date BETWEEN DATE('{ today }') AND DATE('{ today }') + INTERVAL '{ days } days'
                AND     sales.experiment = 'experiment_1'
            GROUP BY    sales.date, ids.region_code, COALESCE(ns.new_product_id, sales.product_id)
        ),

         t_unique AS
        (
            SELECT      DISTINCT 
                        d1.close_date AS close_date,
                        d3.product_id
            FROM        t_clean_demand d1 
            CROSS JOIN  (SELECT DISTINCT d2.product_id FROM t_clean_demand d2) d3
        ),

        t_share AS
        (
            SELECT        o.close_date,
                          COALESCE(ns.new_product_id, op.product_id) AS product_id,
                          SUM(op.quantity * op.price)/SUM(SUM(op.quantity * op.price)) OVER(PARTITION BY o.close_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS share
            FROM          postgres_main_federate."ops_demand_planning.forecast_context.dp_forecast_params_identifiers" forecast_params
            INNER JOIN    postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" input_params ON forecast_params.dp_input_datasets_id = input_params.id
            INNER JOIN    postgres_main_federate."ops_demand_planning.forecast_context.dp_microzone_group" mgroup ON mgroup.microzone_group_code = input_params.microzone_group_code
            INNER JOIN    dpr_shared.dim_site region ON region.identifier_value = input_params.region_code
            INNER JOIN    dpr_shared.dim_microzone mzone ON mzone.site_id = region.site_id AND mzone.microzone_id = mgroup.microzone_id
            INNER JOIN    postgres_main_{ country }."reports.orders" o ON o.region_code = input_params.region_code AND CASE WHEN input_params.warehouse <> 'ALL' THEN o.warehouse = input_params.warehouse ELSE 1 = 1 END
            INNER JOIN    postgres_main_{ country }."reports.orders_products" op ON o.id = op.order_id
            LEFT JOIN     postgres_main_{ country }."reports.sandbox.new_skus" ns ON op.product_id = ns.old_product_id
            INNER JOIN    t_products tp ON tp.product_id = COALESCE(ns.new_product_id, op.product_id)
            WHERE         -- Order Status
                          o.status = 1
                AND       op.is_deleted = 'false'
                          -- Region/Warehouse/Accum_Days
                AND       input_params.region_code = '{ region_code }'
                AND       input_params.warehouse = '{ warehouse }'
                AND       input_params.accumulated_days = 1
                          -- Time filters
                AND       o.close_date BETWEEN DATE('{ today }') AND DATE('{ today }') + INTERVAL '{ days } days'
                AND       o.close_date >= forecast_params.start_day_live
                AND       o.close_date <= COALESCE(forecast_params.end_day_live, CURRENT_DATE + 1000)
                          -- Blacklist
                AND       COALESCE(ns.new_product_id, op.product_id) NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'product_id' AND region_code = '{ region_code }')
                          -- Microzone geometry contains the order location
                AND       st_contains(mzone.boundaries, st_point(o.latitude, o.longitude))
            GROUP BY      o.close_date, COALESCE(ns.new_product_id, op.product_id)
        ),
        
        experiment_1 AS 
        (
            SELECT  m.forecast_date,
                    DATE(m.created_on) AS created_on,
                    m.region_code,
                    m.warehouse,
                    m.dp_forecast_params_id,
                    COALESCE(ns.new_product_id, m.product_id) AS product_id,
                    (DATEDIFF('day', DATE(m.created_on), DATE(m.forecast_date))) - (DATEDIFF('week',DATE(m.created_on), DATE(m.forecast_date)) * 1) AS timeframe,
                    COALESCE(SUM(m.forecast_quantity * COALESCE(ns.old_units_per_new_unit, 1)),0) AS forecast
            FROM    lnd_ops_dev.dp_output_exponential_smoothing_model_plutonio m
            LEFT JOIN   postgres_main_{country}."reports.sandbox.new_skus" ns ON m.product_id = ns.old_product_id
            INNER JOIN  t_products tp ON tp.product_id = COALESCE(ns.new_product_id, m.product_id)
            WHERE   date(m.created_on) = '{today}'
                AND region_code = '{region_code}'
                AND warehouse = '{warehouse}'
                AND experiment = 'experiment_1'
            GROUP BY m.forecast_date, DATE(m.created_on), m.region_code, m.warehouse, m.dp_forecast_params_id, timeframe,
                    COALESCE(ns.new_product_id, m.product_id)
        ),

        experiment_2 AS 
        (
            SELECT  m.forecast_date,
                    DATE(m.created_on) AS created_on,
                    m.region_code,
                    m.warehouse,
                    m.dp_forecast_params_id,
                    COALESCE(ns.new_product_id, m.product_id) AS product_id,
                    (DATEDIFF('day', DATE(m.created_on), DATE(m.forecast_date))) - (DATEDIFF('week',DATE(m.created_on), DATE(m.forecast_date)) * 1) AS timeframe,
                    COALESCE(SUM(m.forecast_quantity * COALESCE(ns.old_units_per_new_unit, 1)),0) AS forecast
            FROM    lnd_ops_dev.dp_output_exponential_smoothing_model_plutonio m
            LEFT JOIN   postgres_main_{country}."reports.sandbox.new_skus" ns ON m.product_id = ns.old_product_id
            INNER JOIN  t_products tp ON tp.product_id = COALESCE(ns.new_product_id, m.product_id)
            WHERE   date(m.created_on) = '{today}'
                AND region_code = '{region_code}'
                AND warehouse = '{warehouse}'
                AND experiment = 'experiment_2'
            GROUP BY m.forecast_date, DATE(m.created_on), m.region_code, m.warehouse, m.dp_forecast_params_id, timeframe,
                    COALESCE(ns.new_product_id, m.product_id)
        ),
        
        experiment_3 AS 
        (
            SELECT  m.forecast_date,
                    DATE(m.created_on) AS created_on,
                    m.region_code,
                    m.warehouse,
                    m.dp_forecast_params_id,
                    COALESCE(ns.new_product_id, m.product_id) AS product_id,
                    (DATEDIFF('day', DATE(m.created_on), DATE(m.forecast_date))) - (DATEDIFF('week',DATE(m.created_on), DATE(m.forecast_date)) * 1) AS timeframe,
                    COALESCE(SUM(m.forecast_quantity * COALESCE(ns.old_units_per_new_unit, 1)),0) AS forecast
            FROM    lnd_ops_dev.dp_output_exponential_smoothing_model_plutonio m
            LEFT JOIN   postgres_main_{country}."reports.sandbox.new_skus" ns ON m.product_id = ns.old_product_id
            INNER JOIN  t_products tp ON tp.product_id = COALESCE(ns.new_product_id, m.product_id)
            WHERE   date(m.created_on) = '{today}'
                AND region_code = '{region_code}'
                AND warehouse = '{warehouse}'
                AND experiment = 'experiment_3'
            GROUP BY m.forecast_date, DATE(m.created_on), m.region_code, m.warehouse, m.dp_forecast_params_id, timeframe,
                    COALESCE(ns.new_product_id, m.product_id)
        ),
        
        experiment_4 AS 
        (
            SELECT  m.forecast_date,
                    DATE(m.created_on) AS created_on,
                    m.region_code,
                    m.warehouse,
                    m.dp_forecast_params_id,
                    COALESCE(ns.new_product_id, m.product_id) AS product_id,
                    (DATEDIFF('day', DATE(m.created_on), DATE(m.forecast_date))) - (DATEDIFF('week',DATE(m.created_on), DATE(m.forecast_date)) * 1) AS timeframe,
                    COALESCE(SUM(m.forecast_quantity * COALESCE(ns.old_units_per_new_unit, 1)),0) AS forecast
            FROM    lnd_ops_dev.dp_output_exponential_smoothing_model_plutonio m
            LEFT JOIN   postgres_main_{country}."reports.sandbox.new_skus" ns ON m.product_id = ns.old_product_id
            INNER JOIN  t_products tp ON tp.product_id = COALESCE(ns.new_product_id, m.product_id)
            WHERE   date(m.created_on) = '{today}'
                AND region_code = '{region_code}'
                AND warehouse = '{warehouse}'
                AND experiment = 'experiment_4'
            GROUP BY m.forecast_date, DATE(m.created_on), m.region_code, m.warehouse, m.dp_forecast_params_id, timeframe,
                    COALESCE(ns.new_product_id, m.product_id)
        ),
        
        experiment_5 AS 
        (
            SELECT  m.forecast_date,
                    DATE(m.created_on) AS created_on,
                    m.region_code,
                    m.warehouse,
                    m.dp_forecast_params_id,
                    COALESCE(ns.new_product_id, m.product_id) AS product_id,
                    (DATEDIFF('day', DATE(m.created_on), DATE(m.forecast_date))) - (DATEDIFF('week',DATE(m.created_on), DATE(m.forecast_date)) * 1) AS timeframe,
                    COALESCE(SUM(m.forecast_quantity * COALESCE(ns.old_units_per_new_unit, 1)),0) AS forecast
            FROM    lnd_ops_dev.dp_output_exponential_smoothing_model_plutonio m
            LEFT JOIN   postgres_main_{country}."reports.sandbox.new_skus" ns ON m.product_id = ns.old_product_id
            INNER JOIN  t_products tp ON tp.product_id = COALESCE(ns.new_product_id, m.product_id)
            WHERE   date(m.created_on) = '{today}'
                AND region_code = '{region_code}'
                AND warehouse = '{warehouse}'
                AND experiment = 'experiment_5'
            GROUP BY m.forecast_date, DATE(m.created_on), m.region_code, m.warehouse, m.dp_forecast_params_id, timeframe,
                    COALESCE(ns.new_product_id, m.product_id)
        ),
        
        experiment_6 AS 
        (
            SELECT  m.forecast_date,
                    DATE(m.created_on) AS created_on,
                    m.region_code,
                    m.warehouse,
                    m.dp_forecast_params_id,
                    COALESCE(ns.new_product_id, m.product_id) AS product_id,
                    (DATEDIFF('day', DATE(m.created_on), DATE(m.forecast_date))) - (DATEDIFF('week',DATE(m.created_on), DATE(m.forecast_date)) * 1) AS timeframe,
                    COALESCE(SUM(m.forecast_quantity * COALESCE(ns.old_units_per_new_unit, 1)),0) AS forecast
            FROM    lnd_ops_dev.dp_output_exponential_smoothing_model_plutonio m
            LEFT JOIN   postgres_main_{country}."reports.sandbox.new_skus" ns ON m.product_id = ns.old_product_id
            INNER JOIN  t_products tp ON tp.product_id = COALESCE(ns.new_product_id, m.product_id)
            WHERE   date(m.created_on) = '{today}'
                AND region_code = '{region_code}'
                AND warehouse = '{warehouse}'
                AND experiment = 'experiment_6'
            GROUP BY m.forecast_date, DATE(m.created_on), m.region_code, m.warehouse, m.dp_forecast_params_id, timeframe,
                    COALESCE(ns.new_product_id, m.product_id)
        ),
        
        experiment_7 AS 
        (
            SELECT  m.forecast_date,
                    DATE(m.created_on) AS created_on,
                    m.region_code,
                    m.warehouse,
                    m.dp_forecast_params_id,
                    COALESCE(ns.new_product_id, m.product_id) AS product_id,
                    (DATEDIFF('day', DATE(m.created_on), DATE(m.forecast_date))) - (DATEDIFF('week',DATE(m.created_on), DATE(m.forecast_date)) * 1) AS timeframe,
                    COALESCE(SUM(m.forecast_quantity * COALESCE(ns.old_units_per_new_unit, 1)),0) AS forecast
            FROM    lnd_ops_dev.dp_output_exponential_smoothing_model_plutonio m
            LEFT JOIN   postgres_main_{country}."reports.sandbox.new_skus" ns ON m.product_id = ns.old_product_id
            INNER JOIN  t_products tp ON tp.product_id = COALESCE(ns.new_product_id, m.product_id)
            WHERE   date(m.created_on) = '{today}'
                AND region_code = '{region_code}'
                AND warehouse = '{warehouse}'
                AND experiment = 'experiment_7'
            GROUP BY m.forecast_date, DATE(m.created_on), m.region_code, m.warehouse, m.dp_forecast_params_id, timeframe,
                    COALESCE(ns.new_product_id, m.product_id)
        ),
        
        experiment_8 AS 
        (
            SELECT  m.forecast_date,
                    DATE(m.created_on) AS created_on,
                    m.region_code,
                    m.warehouse,
                    m.dp_forecast_params_id,
                    COALESCE(ns.new_product_id, m.product_id) AS product_id,
                    (DATEDIFF('day', DATE(m.created_on), DATE(m.forecast_date))) - (DATEDIFF('week',DATE(m.created_on), DATE(m.forecast_date)) * 1) AS timeframe,
                    COALESCE(SUM(m.forecast_quantity * COALESCE(ns.old_units_per_new_unit, 1)),0) AS forecast
            FROM    lnd_ops_dev.dp_output_exponential_smoothing_model_plutonio m
            LEFT JOIN   postgres_main_{country}."reports.sandbox.new_skus" ns ON m.product_id = ns.old_product_id
            INNER JOIN  t_products tp ON tp.product_id = COALESCE(ns.new_product_id, m.product_id)
            WHERE   date(m.created_on) = '{today}'
                AND region_code = '{region_code}'
                AND warehouse = '{warehouse}'
                AND experiment = 'experiment_8'
            GROUP BY m.forecast_date, DATE(m.created_on), m.region_code, m.warehouse, m.dp_forecast_params_id, timeframe,
                    COALESCE(ns.new_product_id, m.product_id)
        ),

        t_forecast_experiments AS 
        (
            SELECT      COALESCE(exp1.forecast_date, exp2.forecast_date, exp3.forecast_date, exp4.forecast_date, exp5.forecast_date, exp6.forecast_date, exp7.forecast_date, exp8.forecast_date) AS forecast_date,
                        COALESCE(exp1.region_code, exp2.region_code, exp3.region_code, exp4.region_code, exp5.region_code, exp6.region_code, exp7.region_code, exp8.region_code) AS region_code,
                        COALESCE(exp1.warehouse, exp2.warehouse, exp3.warehouse, exp4.warehouse, exp5.warehouse, exp6.warehouse, exp7.warehouse, exp8.warehouse) AS warehouse,
                        COALESCE(exp1.product_id, exp2.product_id, exp3.product_id, exp4.product_id, exp5.product_id, exp6.product_id, exp7.product_id, exp8.product_id) AS product_id,
                        COALESCE(exp1.timeframe, exp2.timeframe, exp3.timeframe, exp4.timeframe, exp5.timeframe, exp6.timeframe, exp7.timeframe, exp8.timeframe) AS timeframe,
                        SUM(COALESCE(exp1.forecast, 0)) AS forecast_exp1,
                        SUM(COALESCE(exp2.forecast, 0)) AS forecast_exp2,
                        SUM(COALESCE(exp3.forecast, 0)) AS forecast_exp3,
                        SUM(COALESCE(exp4.forecast, 0)) AS forecast_exp4,
                        SUM(COALESCE(exp5.forecast, 0)) AS forecast_exp5,
                        SUM(COALESCE(exp6.forecast, 0)) AS forecast_exp6,
                        SUM(COALESCE(exp7.forecast, 0)) AS forecast_exp7,
                        SUM(COALESCE(exp8.forecast, 0)) AS forecast_exp8
            FROM        experiment_1 exp1
            FULL OUTER JOIN   experiment_2 exp2 ON exp1.forecast_date = exp2.forecast_date AND exp1.product_id = exp2.product_id AND exp1.timeframe = exp2.timeframe
            FULL OUTER JOIN   experiment_3 exp3 ON exp1.forecast_date = exp3.forecast_date AND exp1.product_id = exp3.product_id AND exp1.timeframe = exp3.timeframe
            FULL OUTER JOIN   experiment_4 exp4 ON exp1.forecast_date = exp4.forecast_date AND exp1.product_id = exp4.product_id AND exp1.timeframe = exp4.timeframe
            FULL OUTER JOIN   experiment_5 exp5 ON exp1.forecast_date = exp5.forecast_date AND exp1.product_id = exp5.product_id AND exp1.timeframe = exp5.timeframe
            FULL OUTER JOIN   experiment_6 exp6 ON exp1.forecast_date = exp6.forecast_date AND exp1.product_id = exp6.product_id AND exp1.timeframe = exp6.timeframe
            FULL OUTER JOIN   experiment_7 exp7 ON exp1.forecast_date = exp7.forecast_date AND exp1.product_id = exp7.product_id AND exp1.timeframe = exp7.timeframe
            FULL OUTER JOIN   experiment_8 exp8 ON exp1.forecast_date = exp8.forecast_date AND exp1.product_id = exp8.product_id AND exp1.timeframe = exp8.timeframe

            GROUP BY    COALESCE(exp1.forecast_date, exp2.forecast_date, exp3.forecast_date, exp4.forecast_date, exp5.forecast_date, exp6.forecast_date, exp7.forecast_date, exp8.forecast_date),
                        COALESCE(exp1.region_code, exp2.region_code, exp3.region_code, exp4.region_code, exp5.region_code, exp6.region_code, exp7.region_code, exp8.region_code),
                        COALESCE(exp1.warehouse, exp2.warehouse, exp3.warehouse, exp4.warehouse, exp5.warehouse, exp6.warehouse, exp7.warehouse, exp8.warehouse),
                        COALESCE(exp1.product_id, exp2.product_id, exp3.product_id, exp4.product_id, exp5.product_id, exp6.product_id, exp7.product_id, exp8.product_id),
                        COALESCE(exp1.timeframe, exp2.timeframe, exp3.timeframe, exp4.timeframe, exp5.timeframe, exp6.timeframe, exp7.timeframe, exp8.timeframe)
        ),

        t_mafe AS 
        (
            SELECT      COALESCE(tcd.close_date, tfe.forecast_date) AS forecast_date,
                        COALESCE(tcd.region_code, tfe.region_code) AS region_code,
                        COALESCE(tfe.warehouse,'{ warehouse }') AS warehouse,
                        COALESCE(tcd.product_id, tfe.product_id) AS product_id,
                        COALESCE(tfe.timeframe, 2) AS timeframe,
                        SUM(COALESCE(demand,0)) AS demand,
                        SUM(COALESCE(tfe.forecast_exp1, 0)) AS forecast_exp1,
                        SUM(COALESCE(tfe.forecast_exp2, 0)) AS forecast_exp2,
                        SUM(COALESCE(tfe.forecast_exp3, 0)) AS forecast_exp3,
                        SUM(COALESCE(tfe.forecast_exp4, 0)) AS forecast_exp4,
                        SUM(COALESCE(tfe.forecast_exp5, 0)) AS forecast_exp5,
                        SUM(COALESCE(tfe.forecast_exp6, 0)) AS forecast_exp6,
                        SUM(COALESCE(tfe.forecast_exp7, 0)) AS forecast_exp7,
                        SUM(COALESCE(tfe.forecast_exp8, 0)) AS forecast_exp8,
                        CASE 
                            WHEN SUM(COALESCE(demand,0)) = 0 AND SUM(COALESCE(tfe.forecast_exp1, 0)) > 0 THEN 100
                            WHEN SUM(COALESCE(demand,0)) = 0 AND SUM(COALESCE(tfe.forecast_exp1, 0)) = 0 THEN 0
                            ELSE 100 * SUM(ABS(COALESCE(tfe.forecast_exp1,0) - COALESCE(demand,0)))/SUM(COALESCE(demand,0)) 
                        END AS mafe_exp1,
                        CASE 
                            WHEN SUM(COALESCE(demand,0)) = 0 AND SUM(COALESCE(tfe.forecast_exp2, 0)) > 0 THEN 100
                            WHEN SUM(COALESCE(demand,0)) = 0 AND SUM(COALESCE(tfe.forecast_exp2, 0)) = 0 THEN 0
                            ELSE 100 * SUM(ABS(COALESCE(tfe.forecast_exp2,0) - COALESCE(demand,0)))/SUM(COALESCE(demand,0)) 
                        END AS mafe_exp2,
                        CASE 
                            WHEN SUM(COALESCE(demand,0)) = 0 AND SUM(COALESCE(tfe.forecast_exp3, 0)) > 0 THEN 100
                            WHEN SUM(COALESCE(demand,0)) = 0 AND SUM(COALESCE(tfe.forecast_exp3, 0)) = 0 THEN 0
                            ELSE 100 * SUM(ABS(COALESCE(tfe.forecast_exp3,0) - COALESCE(demand,0)))/SUM(COALESCE(demand,0)) 
                        END AS mafe_exp3,
                        CASE 
                            WHEN SUM(COALESCE(demand,0)) = 0 AND SUM(COALESCE(tfe.forecast_exp4, 0)) > 0 THEN 100
                            WHEN SUM(COALESCE(demand,0)) = 0 AND SUM(COALESCE(tfe.forecast_exp4, 0)) = 0 THEN 0
                            ELSE 100 * SUM(ABS(COALESCE(tfe.forecast_exp4,0) - COALESCE(demand,0)))/SUM(COALESCE(demand,0)) 
                        END AS mafe_exp4,
                        CASE 
                            WHEN SUM(COALESCE(demand,0)) = 0 AND SUM(COALESCE(tfe.forecast_exp5, 0)) > 0 THEN 100
                            WHEN SUM(COALESCE(demand,0)) = 0 AND SUM(COALESCE(tfe.forecast_exp5, 0)) = 0 THEN 0
                            ELSE 100 * SUM(ABS(COALESCE(tfe.forecast_exp5,0) - COALESCE(demand,0)))/SUM(COALESCE(demand,0)) 
                        END AS mafe_exp5,
                        CASE 
                            WHEN SUM(COALESCE(demand,0)) = 0 AND SUM(COALESCE(tfe.forecast_exp6, 0)) > 0 THEN 100
                            WHEN SUM(COALESCE(demand,0)) = 0 AND SUM(COALESCE(tfe.forecast_exp6, 0)) = 0 THEN 0
                            ELSE 100 * SUM(ABS(COALESCE(tfe.forecast_exp6,0) - COALESCE(demand,0)))/SUM(COALESCE(demand,0)) 
                        END AS mafe_exp6,
                        CASE 
                            WHEN SUM(COALESCE(demand,0)) = 0 AND SUM(COALESCE(tfe.forecast_exp7, 0)) > 0 THEN 100
                            WHEN SUM(COALESCE(demand,0)) = 0 AND SUM(COALESCE(tfe.forecast_exp7, 0)) = 0 THEN 0
                            ELSE 100 * SUM(ABS(COALESCE(tfe.forecast_exp7,0) - COALESCE(demand,0)))/SUM(COALESCE(demand,0)) 
                        END AS mafe_exp7,
                        CASE 
                            WHEN SUM(COALESCE(demand,0)) = 0 AND SUM(COALESCE(tfe.forecast_exp8, 0)) > 0 THEN 100
                            WHEN SUM(COALESCE(demand,0)) = 0 AND SUM(COALESCE(tfe.forecast_exp8, 0)) = 0 THEN 0
                            ELSE 100 * SUM(ABS(COALESCE(tfe.forecast_exp8,0) - COALESCE(demand,0)))/SUM(COALESCE(demand,0)) 
                        END AS mafe_exp8,
                        COALESCE(
                        CASE 
                            WHEN SUM(share) OVER (PARTITION BY COALESCE(tcd.close_date, tfe.forecast_date), COALESCE(tfe.timeframe, 2)) > 0 THEN (share/SUM(share) OVER (PARTITION BY COALESCE(tcd.close_date, tfe.forecast_date), COALESCE(tfe.timeframe, 2))) 
                            ELSE 0
                        END,0) AS share
            FROM        t_forecast_experiments tfe
            INNER JOIN  t_clean_demand tcd ON tcd.product_id = tfe.product_id AND DATE(tcd.close_date) = DATE(tfe.forecast_date)
            LEFT JOIN   t_share gmv ON gmv.close_date = COALESCE(tcd.close_date, tfe.forecast_date) AND gmv.product_id = COALESCE(tcd.product_id, tfe.product_id)

            GROUP BY    COALESCE(tcd.close_date, tfe.forecast_date),
                        COALESCE(tcd.region_code, tfe.region_code),
                        COALESCE(tfe.warehouse,'{ warehouse }'),
                        COALESCE(tcd.product_id, tfe.product_id),
                        COALESCE(tfe.timeframe, 2),
                        gmv.share
        )
        
        SELECT      forecast_date,
                    region_code,
                    warehouse,
                    timeframe,
                    SUM(mafe_exp1 * COALESCE(share,0)) AS mafe_exp1,
                    SUM(mafe_exp2 * COALESCE(share,0)) AS mafe_exp2,
                    SUM(mafe_exp3 * COALESCE(share,0)) AS mafe_exp3,
                    SUM(mafe_exp4 * COALESCE(share,0)) AS mafe_exp4,
                    SUM(mafe_exp5 * COALESCE(share,0)) AS mafe_exp5,
                    SUM(mafe_exp6 * COALESCE(share,0)) AS mafe_exp6,
                    SUM(mafe_exp7 * COALESCE(share,0)) AS mafe_exp7,
                    SUM(mafe_exp8 * COALESCE(share,0)) AS mafe_exp8
                    --AVG(mafe_exp1) AS mafe_exp1,
                    --AVG(mafe_exp2) AS mafe_exp2,
                    --AVG(mafe_exp3) AS mafe_exp3,
                    --AVG(mafe_exp4) AS mafe_exp4,
                    --AVG(mafe_exp5) AS mafe_exp5,
                    --AVG(mafe_exp6) AS mafe_exp6,
                    --AVG(mafe_exp7) AS mafe_exp7,
                    --AVG(mafe_exp8) AS mafe_exp8
        FROM        t_mafe th
        GROUP BY    forecast_date, region_code, warehouse, timeframe
        """
    return query