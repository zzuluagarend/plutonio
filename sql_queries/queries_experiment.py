def exp1(country, dataset_id, restriction=""):
    if restriction:
        restriction = "WHERE " + restriction
    query = f"""
            with
            -------------------------------------------------
            -- List of datasets to build
            -------------------------------------------------
            datasets AS
            (
            SELECT      LOWER(dc.identifier_value) as country
            ,           ds.region_code as region_code
            ,           ds.warehouse AS warehouse
            ,           ds.id as dataset_identifier
            ,           ds.stockout_weight as stockout_weight
            FROM        postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" ds
            INNER JOIN  dpr_shared.dim_site dt ON dt.identifier_value = ds.region_code
            INNER JOIN  dpr_shared.dim_country dc ON dc.country_id = dt.country_id
            WHERE       ds.accumulated_days = 1
                        AND LOWER(dc.identifier_value) = LOWER('{country}')
                        AND ds.id = {dataset_id}
            ),
            
            -------------------------------------------------
            -- DAILY demand, full load, products in orders
            -------------------------------------------------
            orders AS
            (
            SELECT      o.close_date AS close_date
            ,           op.product_id AS order_product_id
            ,           op.quantity AS quantity
            ,           dataset.id AS dp_input_datasets_id
            ,           dataset.region_code AS region_code
            ,           dataset.stockout_weight AS stockout_weight
            FROM        postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" dataset
            INNER JOIN  postgres_main_{country}."reports.orders" o ON o.region_code = dataset.region_code
            INNER JOIN  postgres_main_{country}."reports.orders_products" op ON o.id = op.order_id
            INNER JOIN  postgres_main_federate."ops_demand_planning.forecast_context.dp_microzone_group" mgroup ON mgroup.microzone_group_code = dataset.microzone_group_code
            INNER JOIN  dpr_shared.dim_site region ON region.identifier_value = dataset.region_code
            INNER JOIN  dpr_shared.dim_microzone mzone ON mzone.site_id = region.site_id
                                                   AND mzone.microzone_id = mgroup.microzone_id
            INNER JOIN  datasets datasets_list
                                                    ON datasets_list.dataset_identifier = dataset.id
            LEFT JOIN   postgres_main_federate."ops_wh_planning.customer_configuration" customers ON customers.region = dataset.region_code 
                                                    AND customers.tag_name = o.tag_name 
                                                    AND customers.warehouse_vip = 'true'
            WHERE       -- Order Status
                       o.status = 1
                       AND op.is_deleted = 'false'
                       -- Remove future sales, and sundays (weird cases where close_date is on a sunday, shouldn't ever happen)
                       AND o.close_date <= CURRENT_DATE
                       AND DATE_PART(dow, o.close_date) <> 0
                       -- Blacklist
                       AND o.id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'order_id' AND region_code = dataset.region_code)
                       AND op.product_id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'product_id' AND region_code = dataset.region_code)
                       -- Microzone geometry contains the order location
                       AND (st_contains(mzone.boundaries, st_point(o.latitude, o.longitude))
                            AND (customers.warehouse IS NULL OR dataset.warehouse = 'ALL'))

            UNION ALL

            -- KAM orders 
            SELECT      o.close_date AS close_date
            ,           op.product_id AS order_product_id
            ,           op.quantity AS quantity
            ,           dataset.id AS dp_input_datasets_id
            ,           dataset.region_code AS region_code
            ,           dataset.stockout_weight AS stockout_weight
            FROM        postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" dataset
            INNER JOIN  postgres_main_{country}."reports.orders" o ON o.region_code = dataset.region_code
            INNER JOIN  postgres_main_{country}."reports.orders_products" op ON o.id = op.order_id
            INNER JOIN  datasets datasets_list
                                                    ON datasets_list.dataset_identifier = dataset.id
            INNER JOIN  postgres_main_federate."ops_wh_planning.customer_configuration" customers ON customers.region = dataset.region_code 
                                                    AND customers.tag_name = o.tag_name 
                                                    AND customers.warehouse_vip = 'true'
            WHERE       -- Order Status
                       o.status = 1
                       AND op.is_deleted = 'false'
                       -- Remove future sales, and sundays (weird cases where close_date is on a sunday, shouldn't ever happen)
                       AND o.close_date <= CURRENT_DATE
                       AND DATE_PART(dow, o.close_date) <> 0
                       -- Blacklist
                       AND o.id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'order_id' AND region_code = dataset.region_code)
                       AND op.product_id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'product_id' AND region_code = dataset.region_code)
                       -- KAM conditions
                       AND customers.region IS NOT NULL
                       AND dataset.warehouse <> 'ALL'
                       AND dataset.warehouse = customers.warehouse
            ),
            
            -------------------------------------------------
            -- Demand with outliers removed 
            -------------------------------------------------
            demand AS
            (
            SELECT      orders.close_date AS order_close_date
            ,           COALESCE(ns.new_product_id, orders.order_product_id) AS demand_product_id
            ,           orders.dp_input_datasets_id AS demand_dp_input_datasets_id
            ,           orders.region_code AS region_code
            ,           orders.stockout_weight AS stockout_weight
            ,           SUM(orders.quantity * COALESCE(ns.old_units_per_new_unit, 1) ) AS quantity
            FROM        orders
            LEFT JOIN   postgres_main_{country}."reports.sandbox.new_skus" ns ON orders.order_product_id = ns.old_product_id
            GROUP BY    order_close_date, demand_product_id, demand_dp_input_datasets_id, region_code, stockout_weight
            ),
            
            -------------------------------------------------
            -- Stockouts
            -------------------------------------------------
            stockouts AS
            (
            SELECT      fs.close_date AS stockout_close_date
            ,           COALESCE(ns.new_product_id,fs.product_id) AS stockout_product_id
            ,           fs.region_code AS region_code
            ,           datasets_list.dataset_identifier AS dataset_identifier
            ,           SUM(fs.stockout_demand * COALESCE(ns.old_units_per_new_unit,  1) * datasets_list.stockout_weight) AS stockouts
            FROM        lnd_ops.dp_output_forecast_stockouts_by_wh fs
            INNER JOIN  datasets datasets_list ON datasets_list.region_code = fs.region_code AND datasets_list.warehouse = fs.warehouse
            INNER JOIN  dpr_shared.dim_site dt ON dt.identifier_value = fs.region_code
            INNER JOIN  dpr_shared.dim_country dc ON dc.country_id = dt.country_id
            LEFT JOIN   postgres_main_{country}."reports.sandbox.new_skus" ns ON fs.product_id = ns.old_product_id
            WHERE       fs.close_date <= CURRENT_DATE
                        AND LOWER(dc.identifier_value) = LOWER('{country}')
            GROUP BY    stockout_close_date, stockout_product_id, fs.region_code, dataset_identifier
            HAVING      stockouts > 0--;
            )
            
            -------------------------------------------------
            -- Demand to ingest
            -------------------------------------------------
            SELECT          COALESCE(demand.order_close_date, souts.stockout_close_date) AS date
            ,               COALESCE(demand.demand_product_id, souts.stockout_product_id) AS product_id
            ,               SUM(COALESCE(demand.quantity,0) + COALESCE(souts.stockouts, 0)) AS demand
            ,               COALESCE(demand.demand_dp_input_datasets_id, souts.dataset_identifier) AS dp_input_datasets_id
            ,               'experiment_1' AS experiment
            FROM            demand 
            FULL OUTER JOIN stockouts souts 
                            ON souts.dataset_identifier = demand.demand_dp_input_datasets_id
                            AND souts.stockout_product_id * 1 = demand.demand_product_id * 1
                            AND DATE(souts.stockout_close_date) = DATE(demand.order_close_date)
            {restriction}
            GROUP BY        date, product_id, dp_input_datasets_id
            """
    return query

def exp2(country, dataset_id, restriction=""):
    if restriction:
        restriction = "WHERE " + restriction
    query = f"""
            WITH
            -------------------------------------------------
            -- List of datasets to build
            -------------------------------------------------
            datasets AS
            (
            SELECT      LOWER(dc.identifier_value) as country
            ,           ds.region_code as region_code
            ,           ds.warehouse AS warehouse
            ,           ds.id as dataset_identifier
            ,           ds.stockout_weight as stockout_weight
            FROM        postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" ds
            INNER JOIN  dpr_shared.dim_site dt ON dt.identifier_value = ds.region_code
            INNER JOIN  dpr_shared.dim_country dc ON dc.country_id = dt.country_id
            WHERE       ds.accumulated_days = 1
                        AND LOWER(dc.identifier_value) = LOWER('{country}')
                        AND ds.id = {dataset_id}
            ),
            
            -------------------------------------------------
            -- DAILY demand, full load, products in orders
            -------------------------------------------------
            orders AS
            (
            SELECT      o.close_date AS close_date
            ,           op.product_id AS order_product_id
            ,           op.quantity AS quantity
            ,           dataset.id AS dp_input_datasets_id
            ,           dataset.region_code AS region_code
            ,           dataset.stockout_weight AS stockout_weight
            FROM        postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" dataset
            INNER JOIN  postgres_main_{country}."reports.orders" o ON o.region_code = dataset.region_code
            INNER JOIN  postgres_main_{country}."reports.orders_products" op ON o.id = op.order_id
            INNER JOIN  postgres_main_federate."ops_demand_planning.forecast_context.dp_microzone_group" mgroup ON mgroup.microzone_group_code = dataset.microzone_group_code
            INNER JOIN  dpr_shared.dim_site region ON region.identifier_value = dataset.region_code
            INNER JOIN  dpr_shared.dim_microzone mzone ON mzone.site_id = region.site_id
                                                   AND mzone.microzone_id = mgroup.microzone_id
            INNER JOIN  datasets datasets_list
                                                    ON datasets_list.dataset_identifier = dataset.id
            LEFT JOIN   postgres_main_federate."ops_wh_planning.customer_configuration" customers ON customers.region = dataset.region_code 
                                                    AND customers.tag_name = o.tag_name 
                                                    AND customers.warehouse_vip = 'true'
            WHERE       -- Order Status
                       o.status = 1
                       AND op.is_deleted = 'false'
                       -- Remove future sales, and sundays (weird cases where close_date is on a sunday, shouldn't ever happen)
                       AND o.close_date <= CURRENT_DATE
                       AND DATE_PART(dow, o.close_date) <> 0
                       -- Blacklist
                       AND o.id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'order_id' AND region_code = dataset.region_code)
                       AND op.product_id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'product_id' AND region_code = dataset.region_code)
                       -- Microzone geometry contains the order location
                       AND (st_contains(mzone.boundaries, st_point(o.latitude, o.longitude))
                            AND (customers.warehouse IS NULL OR dataset.warehouse = 'ALL'))

            UNION ALL

            -- Filtros
            SELECT      o.close_date AS close_date
            ,           op.product_id AS order_product_id
            ,           op.quantity AS quantity
            ,           dataset.id AS dp_input_datasets_id
            ,           dataset.region_code AS region_code
            ,           dataset.stockout_weight AS stockout_weight
            FROM        postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" dataset
            INNER JOIN  postgres_main_{country}."reports.orders" o ON o.region_code = dataset.region_code
            INNER JOIN  postgres_main_{country}."reports.orders_products" op ON o.id = op.order_id
            INNER JOIN  datasets datasets_list
                                                    ON datasets_list.dataset_identifier = dataset.id
            INNER JOIN  postgres_main_federate."ops_wh_planning.customer_configuration" customers ON customers.region = dataset.region_code 
                                                    AND customers.tag_name = o.tag_name 
                                                    AND customers.warehouse_vip = 'true'
            WHERE       -- Order Status
                       o.status = 1
                       AND op.is_deleted = 'false'
                       -- Remove future sales, and sundays (weird cases where close_date is on a sunday, shouldn't ever happen)
                       AND o.close_date <= CURRENT_DATE
                       AND DATE_PART(dow, o.close_date) <> 0
                       -- Blacklist
                       AND o.id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'order_id' AND region_code = dataset.region_code)
                       AND op.product_id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'product_id' AND region_code = dataset.region_code)
                       -- KAM conditions
                       AND customers.region IS NOT NULL
                       AND dataset.warehouse <> 'ALL'
                       AND dataset.warehouse = customers.warehouse
            ),
            
            -------------------------------------------------
            -- Mean and Standard Deviation of Product Orders
            -------------------------------------------------
            deviation AS
            (
            SELECT      order_product_id AS deviation_product_id
            ,           dp_input_datasets_id AS dp_input_datasets_id
            ,           AVG(quantity) AS average
            ,           COALESCE(stddev(quantity), 0) AS stadDev
            FROM        orders products_demand
            GROUP BY    dp_input_datasets_id, deviation_product_id
            ),
            
            -------------------------------------------------
            -- Demand with outliers removed 
            -------------------------------------------------
            demand AS
            (
            SELECT      orders.close_date AS order_close_date
            ,           COALESCE(ns.new_product_id, orders.order_product_id) AS demand_product_id
            ,           orders.dp_input_datasets_id AS demand_dp_input_datasets_id
            ,           orders.region_code AS region_code
            ,           orders.stockout_weight AS stockout_weight
            ,           SUM(orders.quantity * COALESCE(ns.old_units_per_new_unit, 1) ) AS quantity
            FROM        orders
            LEFT JOIN   postgres_main_{country}."reports.sandbox.new_skus" ns ON orders.order_product_id = ns.old_product_id
            LEFT JOIN   deviation 
                                ON orders.dp_input_datasets_id = deviation.dp_input_datasets_id
                                AND orders.order_product_id = deviation.deviation_product_id
            WHERE       orders.quantity <= (deviation.average + deviation.stadDev * 6)
            GROUP BY    order_close_date, demand_product_id, demand_dp_input_datasets_id, region_code, stockout_weight
            ),
            
            -------------------------------------------------
            -- Stockouts
            -------------------------------------------------
            stockouts AS
            (
            SELECT      fs.close_date AS stockout_close_date
            ,           COALESCE(ns.new_product_id,fs.product_id) AS stockout_product_id
            ,           fs.region_code AS region_code
            ,           datasets_list.dataset_identifier AS dataset_identifier
            ,           SUM(fs.stockout_demand * COALESCE(ns.old_units_per_new_unit,  1) * datasets_list.stockout_weight) AS stockouts
            FROM        lnd_ops.dp_output_forecast_stockouts_by_wh fs
            INNER JOIN  datasets datasets_list ON datasets_list.region_code = fs.region_code AND datasets_list.warehouse = fs.warehouse
            INNER JOIN  dpr_shared.dim_site dt ON dt.identifier_value = fs.region_code
            INNER JOIN  dpr_shared.dim_country dc ON dc.country_id = dt.country_id
            LEFT JOIN   postgres_main_{country}."reports.sandbox.new_skus" ns ON fs.product_id = ns.old_product_id
            WHERE       fs.close_date <= CURRENT_DATE
                        AND LOWER(dc.identifier_value) = LOWER('{country}')
            GROUP BY    stockout_close_date, stockout_product_id, fs.region_code, dataset_identifier
            HAVING      stockouts > 0
            ),
            
            -------------------------------------------------
            -- Demand per day
            -------------------------------------------------
            demand_by_day AS
            (
            SELECT          COALESCE(demand.order_close_date, souts.stockout_close_date) AS date
            ,               COALESCE(demand.demand_product_id, souts.stockout_product_id) AS product_id
            ,               SUM(COALESCE(demand.quantity,0) + COALESCE(souts.stockouts, 0)) AS demand
            ,               COALESCE(demand.demand_dp_input_datasets_id, souts.dataset_identifier) AS dp_input_datasets_id
            FROM            demand 
            FULL OUTER JOIN stockouts souts 
                            ON souts.dataset_identifier = demand.demand_dp_input_datasets_id
                            AND souts.stockout_product_id * 1 = demand.demand_product_id * 1
                            AND DATE(souts.stockout_close_date) = DATE(demand.order_close_date)
            GROUP BY        date, product_id, dp_input_datasets_id

            ),
            
            -------------------------------------------------
            -- z-score calculation
            -------------------------------------------------
            z_score_day AS
            (
            SELECT          date,
                            product_id,
                            demand,
                            ISNULL((demand - AVG(demand) OVER(PARTITION BY product_id, dp_input_datasets_id))/NULLIF(stddev(demand) OVER (PARTITION BY product_id, dp_input_datasets_id),0),0) AS z_score,
                            dp_input_datasets_id
            FROM            demand_by_day
            )
            
            -------------------------------------------------
            -- Demand to ingest
            -------------------------------------------------
            
            SELECT          date,
                            product_id,
                            CASE 
                                WHEN z_score >= 2.81 OR z_score <= -2.81 THEN AVG(demand) OVER (PARTITION BY product_id, dp_input_datasets_id)
                                ELSE demand
                            END AS demand,
                            dp_input_datasets_id,
                            'experiment_2' AS experiment
            FROM            z_score_day
            {restriction}
            """
    return query


def exp3(country, dataset_id, restriction=""):
    if restriction:
        restriction = "WHERE " + restriction
    query = f"""
            WITH
            -------------------------------------------------
            -- List of datasets to build
            -------------------------------------------------
            datasets AS
            (
            SELECT      LOWER(dc.identifier_value) as country
            ,           ds.region_code as region_code
            ,           ds.warehouse AS warehouse
            ,           ds.id as dataset_identifier
            ,           ds.stockout_weight as stockout_weight
            FROM        postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" ds
            INNER JOIN  dpr_shared.dim_site dt ON dt.identifier_value = ds.region_code
            INNER JOIN  dpr_shared.dim_country dc ON dc.country_id = dt.country_id
            WHERE       ds.accumulated_days = 1
                        AND LOWER(dc.identifier_value) = LOWER('{country}')
                        AND ds.id = {dataset_id}
            ),
            
            -------------------------------------------------
            -- DAILY demand, full load, products in orders
            -------------------------------------------------
            orders AS
            (
            SELECT      o.close_date AS close_date
            ,           op.product_id AS order_product_id
            ,           op.quantity AS quantity
            ,           dataset.id AS dp_input_datasets_id
            ,           dataset.region_code AS region_code
            ,           dataset.stockout_weight AS stockout_weight
            FROM        postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" dataset
            INNER JOIN  postgres_main_{country}."reports.orders" o ON o.region_code = dataset.region_code
            INNER JOIN  postgres_main_{country}."reports.orders_products" op ON o.id = op.order_id
            INNER JOIN  postgres_main_federate."ops_demand_planning.forecast_context.dp_microzone_group" mgroup ON mgroup.microzone_group_code = dataset.microzone_group_code
            INNER JOIN  dpr_shared.dim_site region ON region.identifier_value = dataset.region_code
            INNER JOIN  dpr_shared.dim_microzone mzone ON mzone.site_id = region.site_id
                                                   AND mzone.microzone_id = mgroup.microzone_id
            INNER JOIN  datasets datasets_list
                                                    ON datasets_list.dataset_identifier = dataset.id
            LEFT JOIN   postgres_main_federate."ops_wh_planning.customer_configuration" customers ON customers.region = dataset.region_code 
                                                    AND customers.tag_name = o.tag_name 
                                                    AND customers.warehouse_vip = 'true'
            WHERE       -- Order Status
                       o.status = 1
                       AND op.is_deleted = 'false'
                       -- Remove future sales, and sundays (weird cases where close_date is on a sunday, shouldn't ever happen)
                       AND o.close_date <= CURRENT_DATE
                       AND DATE_PART(dow, o.close_date) <> 0
                       -- Blacklist
                       AND o.id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'order_id' AND region_code = dataset.region_code)
                       AND op.product_id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'product_id' AND region_code = dataset.region_code)
                       -- Microzone geometry contains the order location
                       AND (st_contains(mzone.boundaries, st_point(o.latitude, o.longitude))
                            AND (customers.warehouse IS NULL OR dataset.warehouse = 'ALL'))

            UNION ALL

            -- Filtros
            SELECT      o.close_date AS close_date
            ,           op.product_id AS order_product_id
            ,           op.quantity AS quantity
            ,           dataset.id AS dp_input_datasets_id
            ,           dataset.region_code AS region_code
            ,           dataset.stockout_weight AS stockout_weight
            FROM        postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" dataset
            INNER JOIN  postgres_main_{country}."reports.orders" o ON o.region_code = dataset.region_code
            INNER JOIN  postgres_main_{country}."reports.orders_products" op ON o.id = op.order_id
            INNER JOIN  datasets datasets_list
                                                    ON datasets_list.dataset_identifier = dataset.id
            INNER JOIN  postgres_main_federate."ops_wh_planning.customer_configuration" customers ON customers.region = dataset.region_code 
                                                    AND customers.tag_name = o.tag_name 
                                                    AND customers.warehouse_vip = 'true'
            WHERE       -- Order Status
                       o.status = 1
                       AND op.is_deleted = 'false'
                       -- Remove future sales, and sundays (weird cases where close_date is on a sunday, shouldn't ever happen)
                       AND o.close_date <= CURRENT_DATE
                       AND DATE_PART(dow, o.close_date) <> 0
                       -- Blacklist
                       AND o.id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'order_id' AND region_code = dataset.region_code)
                       AND op.product_id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'product_id' AND region_code = dataset.region_code)
                       -- KAM conditions
                       AND customers.region IS NOT NULL
                       AND dataset.warehouse <> 'ALL'
                       AND dataset.warehouse = customers.warehouse
            ),
            
            -------------------------------------------------
            -- Percentile 99 of Product Orders
            -------------------------------------------------
            percentile AS
            (
            SELECT      order_product_id AS percentile_product_id
            ,           dp_input_datasets_id AS dp_input_datasets_id
            ,           PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY quantity) AS percentile_99
            FROM        orders products_demand
            GROUP BY    dp_input_datasets_id, percentile_product_id
            ),
                        
            -------------------------------------------------
            -- Demand with outliers removed by percentile orders
            -------------------------------------------------
            demand AS
            (
            SELECT      orders.close_date AS order_close_date
            ,           COALESCE(ns.new_product_id, orders.order_product_id) AS demand_product_id
            ,           orders.dp_input_datasets_id AS demand_dp_input_datasets_id
            ,           orders.region_code AS region_code
            ,           orders.stockout_weight AS stockout_weight
            ,           SUM(orders.quantity * COALESCE(ns.old_units_per_new_unit, 1) ) AS quantity
            FROM        orders
            LEFT JOIN   postgres_main_{country}."reports.sandbox.new_skus" ns ON orders.order_product_id = ns.old_product_id
            LEFT JOIN   percentile 
                                ON orders.dp_input_datasets_id = percentile.dp_input_datasets_id
                                AND orders.order_product_id = percentile.percentile_product_id
            WHERE       orders.quantity <= percentile.percentile_99
            GROUP BY    order_close_date, demand_product_id, demand_dp_input_datasets_id, region_code, stockout_weight
            ),
            
            -------------------------------------------------
            -- Stockouts
            -------------------------------------------------
            stockouts AS
            (
            SELECT      fs.close_date AS stockout_close_date
            ,           COALESCE(ns.new_product_id,fs.product_id) AS stockout_product_id
            ,           fs.region_code AS region_code
            ,           datasets_list.dataset_identifier AS dataset_identifier
            ,           SUM(fs.stockout_demand * COALESCE(ns.old_units_per_new_unit,  1) * datasets_list.stockout_weight) AS stockouts
            FROM        lnd_ops.dp_output_forecast_stockouts_by_wh fs
            INNER JOIN  datasets datasets_list ON datasets_list.region_code = fs.region_code AND datasets_list.warehouse = fs.warehouse
            INNER JOIN  dpr_shared.dim_site dt ON dt.identifier_value = fs.region_code
            INNER JOIN  dpr_shared.dim_country dc ON dc.country_id = dt.country_id
            LEFT JOIN   postgres_main_{country}."reports.sandbox.new_skus" ns ON fs.product_id = ns.old_product_id
            WHERE       fs.close_date <= CURRENT_DATE
                        AND LOWER(dc.identifier_value) = LOWER('{country}')
            GROUP BY    stockout_close_date, stockout_product_id, fs.region_code, dataset_identifier
            HAVING      stockouts > 0--;
            ),
            
            -------------------------------------------------
            -- Demand per day
            -------------------------------------------------
            demand_by_day AS
            (
            SELECT          COALESCE(demand.order_close_date, souts.stockout_close_date) AS date
            ,               COALESCE(demand.demand_product_id, souts.stockout_product_id) AS product_id
            ,               SUM(COALESCE(demand.quantity,0) + COALESCE(souts.stockouts, 0)) AS demand
            ,               COALESCE(demand.demand_dp_input_datasets_id, souts.dataset_identifier) AS dp_input_datasets_id
            FROM            demand 
            FULL OUTER JOIN stockouts souts 
                            ON souts.dataset_identifier = demand.demand_dp_input_datasets_id
                            AND souts.stockout_product_id * 1 = demand.demand_product_id * 1
                            AND DATE(souts.stockout_close_date) = DATE(demand.order_close_date)
            GROUP BY        date, product_id, dp_input_datasets_id

            ),
            
            -------------------------------------------------
            -- z-score calculation
            -------------------------------------------------
            z_score_day AS
            (
            SELECT          date,
                            product_id,
                            demand,
                            ISNULL((demand - AVG(demand) OVER(PARTITION BY product_id, dp_input_datasets_id))/NULLIF(stddev(demand) OVER (PARTITION BY product_id, dp_input_datasets_id),0),0) AS z_score,
                            dp_input_datasets_id
            FROM            demand_by_day
            --GROUP BY        date, product_id, dp_input_datasets_id, demand
            )
            
            -------------------------------------------------
            -- Final calculation
            -------------------------------------------------
            
            SELECT          date,
                            product_id,
                            CASE 
                                WHEN z_score >= 2.81 OR z_score <= -2.81 THEN AVG(demand) OVER (PARTITION BY product_id, dp_input_datasets_id)
                                ELSE demand
                            END AS demand,
                            dp_input_datasets_id,
                            'experiment_3' AS experiment
            FROM            z_score_day
            {restriction}
            """
    return query

def exp4(country, dataset_id,restriction=""):
    if restriction:
        restriction = "WHERE " + restriction
    query = f"""
            WITH
            -------------------------------------------------
            -- List of datasets to build
            -------------------------------------------------
            datasets AS
            (
            SELECT      LOWER(dc.identifier_value) as country
            ,           ds.region_code as region_code
            ,           ds.warehouse AS warehouse
            ,           ds.id as dataset_identifier
            ,           ds.stockout_weight as stockout_weight
            FROM        postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" ds
            INNER JOIN  dpr_shared.dim_site dt ON dt.identifier_value = ds.region_code
            INNER JOIN  dpr_shared.dim_country dc ON dc.country_id = dt.country_id
            WHERE       ds.accumulated_days = 1
                        AND LOWER(dc.identifier_value) = LOWER('{country}')
                        AND ds.id = {dataset_id}
            ),
            
            -------------------------------------------------
            -- DAILY demand, full load, products in orders
            -------------------------------------------------
            orders AS
            (
            SELECT      o.close_date AS close_date
            ,           op.product_id AS order_product_id
            ,           op.quantity AS quantity
            ,           dataset.id AS dp_input_datasets_id
            ,           dataset.region_code AS region_code
            ,           dataset.stockout_weight AS stockout_weight
            FROM        postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" dataset
            INNER JOIN  postgres_main_{country}."reports.orders" o ON o.region_code = dataset.region_code
            INNER JOIN  postgres_main_{country}."reports.orders_products" op ON o.id = op.order_id
            INNER JOIN  postgres_main_federate."ops_demand_planning.forecast_context.dp_microzone_group" mgroup ON mgroup.microzone_group_code = dataset.microzone_group_code
            INNER JOIN  dpr_shared.dim_site region ON region.identifier_value = dataset.region_code
            INNER JOIN  dpr_shared.dim_microzone mzone ON mzone.site_id = region.site_id
                                                   AND mzone.microzone_id = mgroup.microzone_id
            INNER JOIN  datasets datasets_list
                                                    ON datasets_list.dataset_identifier = dataset.id
            LEFT JOIN   postgres_main_federate."ops_wh_planning.customer_configuration" customers ON customers.region = dataset.region_code 
                                                    AND customers.tag_name = o.tag_name 
                                                    AND customers.warehouse_vip = 'true'
            WHERE       -- Order Status
                       o.status = 1
                       AND op.is_deleted = 'false'
                       -- Remove future sales, and sundays (weird cases where close_date is on a sunday, shouldn't ever happen)
                       AND o.close_date <= CURRENT_DATE
                       AND DATE_PART(dow, o.close_date) <> 0
                       -- Blacklist
                       AND o.id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'order_id' AND region_code = dataset.region_code)
                       AND op.product_id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'product_id' AND region_code = dataset.region_code)
                       -- Microzone geometry contains the order location
                       AND (st_contains(mzone.boundaries, st_point(o.latitude, o.longitude))
                            AND (customers.warehouse IS NULL OR dataset.warehouse = 'ALL'))

            UNION ALL

            -- Filtros
            SELECT      o.close_date AS close_date
            ,           op.product_id AS order_product_id
            ,           op.quantity AS quantity
            ,           dataset.id AS dp_input_datasets_id
            ,           dataset.region_code AS region_code
            ,           dataset.stockout_weight AS stockout_weight
            FROM        postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" dataset
            INNER JOIN  postgres_main_{country}."reports.orders" o ON o.region_code = dataset.region_code
            INNER JOIN  postgres_main_{country}."reports.orders_products" op ON o.id = op.order_id
            INNER JOIN  datasets datasets_list
                                                    ON datasets_list.dataset_identifier = dataset.id
            INNER JOIN  postgres_main_federate."ops_wh_planning.customer_configuration" customers ON customers.region = dataset.region_code 
                                                    AND customers.tag_name = o.tag_name 
                                                    AND customers.warehouse_vip = 'true'
            WHERE       -- Order Status
                       o.status = 1
                       AND op.is_deleted = 'false'
                       -- Remove future sales, and sundays (weird cases where close_date is on a sunday, shouldn't ever happen)
                       AND o.close_date <= CURRENT_DATE
                       AND DATE_PART(dow, o.close_date) <> 0
                       -- Blacklist
                       AND o.id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'order_id' AND region_code = dataset.region_code)
                       AND op.product_id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'product_id' AND region_code = dataset.region_code)
                       -- KAM conditions
                       AND customers.region IS NOT NULL
                       AND dataset.warehouse <> 'ALL'
                       AND dataset.warehouse = customers.warehouse
            ),
            
            -------------------------------------------------
            -- Mean and Standard Deviation of Product Orders
            -------------------------------------------------
            deviation AS
            (
            SELECT      order_product_id AS deviation_product_id
            ,           dp_input_datasets_id AS dp_input_datasets_id
            ,           AVG(quantity) AS average
            ,           COALESCE(stddev(quantity), 0) AS stadDev
            FROM        orders products_demand
            GROUP BY    dp_input_datasets_id, deviation_product_id
            ),
            
            -------------------------------------------------
            -- Percentile 99 of Product Orders
            -------------------------------------------------
            percentile AS
            (
            SELECT      order_product_id AS percentile_product_id
            ,           dp_input_datasets_id AS dp_input_datasets_id
            ,           PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY quantity) AS percentile_99
            FROM        orders products_demand
            GROUP BY    dp_input_datasets_id, percentile_product_id
            ),
                        
            -------------------------------------------------
            -- Demand with outliers removed by percentile orders and 6 stddev
            -------------------------------------------------
            demand AS
            (
            SELECT      orders.close_date AS order_close_date
            ,           COALESCE(ns.new_product_id, orders.order_product_id) AS demand_product_id
            ,           orders.dp_input_datasets_id AS demand_dp_input_datasets_id
            ,           orders.region_code AS region_code
            ,           orders.stockout_weight AS stockout_weight
            ,           SUM(orders.quantity * COALESCE(ns.old_units_per_new_unit, 1) ) AS quantity
            FROM        orders
            LEFT JOIN   postgres_main_{country}."reports.sandbox.new_skus" ns ON orders.order_product_id = ns.old_product_id
            LEFT JOIN   deviation 
                                ON orders.dp_input_datasets_id = deviation.dp_input_datasets_id
                                AND orders.order_product_id = deviation.deviation_product_id
            LEFT JOIN   percentile 
                                ON orders.dp_input_datasets_id = percentile.dp_input_datasets_id
                                AND orders.order_product_id = percentile.percentile_product_id
            WHERE       orders.quantity <= (deviation.average + deviation.stadDev * 6)
                AND     orders.quantity <= percentile.percentile_99
            GROUP BY    order_close_date, demand_product_id, demand_dp_input_datasets_id, region_code, stockout_weight
            ),
            
            -------------------------------------------------
            -- Stockouts
            -------------------------------------------------
            stockouts AS
            (
            SELECT      fs.close_date AS stockout_close_date
            ,           COALESCE(ns.new_product_id,fs.product_id) AS stockout_product_id
            ,           fs.region_code AS region_code
            ,           datasets_list.dataset_identifier AS dataset_identifier
            ,           SUM(fs.stockout_demand * COALESCE(ns.old_units_per_new_unit,  1) * datasets_list.stockout_weight) AS stockouts
            FROM        lnd_ops.dp_output_forecast_stockouts_by_wh fs
            INNER JOIN  datasets datasets_list ON datasets_list.region_code = fs.region_code AND datasets_list.warehouse = fs.warehouse
            INNER JOIN  dpr_shared.dim_site dt ON dt.identifier_value = fs.region_code
            INNER JOIN  dpr_shared.dim_country dc ON dc.country_id = dt.country_id
            LEFT JOIN   postgres_main_{country}."reports.sandbox.new_skus" ns ON fs.product_id = ns.old_product_id
            WHERE       fs.close_date <= CURRENT_DATE
                        AND LOWER(dc.identifier_value) = LOWER('{country}')
            GROUP BY    stockout_close_date, stockout_product_id, fs.region_code, dataset_identifier
            HAVING      stockouts > 0--;
            )
            
            -------------------------------------------------
            -- Demand to ingest
            -------------------------------------------------
            SELECT          COALESCE(demand.order_close_date, souts.stockout_close_date) AS date
            ,               COALESCE(demand.demand_product_id, souts.stockout_product_id) AS product_id
            ,               SUM(COALESCE(demand.quantity,0) + COALESCE(souts.stockouts, 0)) AS demand
            ,               COALESCE(demand.demand_dp_input_datasets_id, souts.dataset_identifier) AS dp_input_datasets_id
            ,               'experiment_4' AS experiment
            FROM            demand 
            FULL OUTER JOIN stockouts souts 
                            ON souts.dataset_identifier = demand.demand_dp_input_datasets_id
                            AND souts.stockout_product_id * 1 = demand.demand_product_id * 1
                            AND DATE(souts.stockout_close_date) = DATE(demand.order_close_date)
            {restriction}
            GROUP BY        date, product_id, dp_input_datasets_id
            """
    return query

def exp5(country, dataset_id, restriction=""):
    if restriction:
        restriction = "WHERE " + restriction
    query = f"""
            WITH
            -------------------------------------------------
            -- List of datasets to build
            -------------------------------------------------
            datasets AS
            (
            SELECT      LOWER(dc.identifier_value) as country
            ,           ds.region_code as region_code
            ,           ds.warehouse AS warehouse
            ,           ds.id as dataset_identifier
            ,           ds.stockout_weight as stockout_weight
            FROM        postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" ds
            INNER JOIN  dpr_shared.dim_site dt ON dt.identifier_value = ds.region_code
            INNER JOIN  dpr_shared.dim_country dc ON dc.country_id = dt.country_id
            WHERE       ds.accumulated_days = 1
                        AND LOWER(dc.identifier_value) = LOWER('{country}')
                        AND ds.id = {dataset_id}
            ),
            
            -------------------------------------------------
            -- DAILY demand, full load, products in orders
            -------------------------------------------------
            orders AS
            (
            SELECT      o.close_date AS close_date
            ,           op.product_id AS order_product_id
            ,           op.quantity AS quantity
            ,           dataset.id AS dp_input_datasets_id
            ,           dataset.region_code AS region_code
            ,           dataset.stockout_weight AS stockout_weight
            FROM        postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" dataset
            INNER JOIN  postgres_main_{country}."reports.orders" o ON o.region_code = dataset.region_code
            INNER JOIN  postgres_main_{country}."reports.orders_products" op ON o.id = op.order_id
            INNER JOIN  postgres_main_federate."ops_demand_planning.forecast_context.dp_microzone_group" mgroup ON mgroup.microzone_group_code = dataset.microzone_group_code
            INNER JOIN  dpr_shared.dim_site region ON region.identifier_value = dataset.region_code
            INNER JOIN  dpr_shared.dim_microzone mzone ON mzone.site_id = region.site_id
                                                   AND mzone.microzone_id = mgroup.microzone_id
            INNER JOIN  datasets datasets_list
                                                    ON datasets_list.dataset_identifier = dataset.id
            LEFT JOIN   postgres_main_federate."ops_wh_planning.customer_configuration" customers ON customers.region = dataset.region_code 
                                                    AND customers.tag_name = o.tag_name 
                                                    AND customers.warehouse_vip = 'true'
            WHERE       -- Order Status
                       o.status = 1
                       AND op.is_deleted = 'false'
                       -- Remove future sales, and sundays (weird cases where close_date is on a sunday, shouldn't ever happen)
                       AND o.close_date <= CURRENT_DATE
                       AND DATE_PART(dow, o.close_date) <> 0
                       -- Blacklist
                       AND o.user_id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'user_id' AND region_code = dataset.region_code)
                       AND o.id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'order_id' AND region_code = dataset.region_code)
                       AND op.product_id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'product_id' AND region_code = dataset.region_code)
                       -- Microzone geometry contains the order location
                       AND (st_contains(mzone.boundaries, st_point(o.latitude, o.longitude))
                            AND (customers.warehouse IS NULL OR dataset.warehouse = 'ALL'))

            UNION ALL

            -- KAM orders 
            SELECT      o.close_date AS close_date
            ,           op.product_id AS order_product_id
            ,           op.quantity AS quantity
            ,           dataset.id AS dp_input_datasets_id
            ,           dataset.region_code AS region_code
            ,           dataset.stockout_weight AS stockout_weight
            FROM        postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" dataset
            INNER JOIN  postgres_main_{country}."reports.orders" o ON o.region_code = dataset.region_code
            INNER JOIN  postgres_main_{country}."reports.orders_products" op ON o.id = op.order_id
            INNER JOIN  datasets datasets_list
                                                    ON datasets_list.dataset_identifier = dataset.id
            INNER JOIN  postgres_main_federate."ops_wh_planning.customer_configuration" customers ON customers.region = dataset.region_code 
                                                    AND customers.tag_name = o.tag_name 
                                                    AND customers.warehouse_vip = 'true'
            WHERE       -- Order Status
                       o.status = 1
                       AND op.is_deleted = 'false'
                       -- Remove future sales, and sundays (weird cases where close_date is on a sunday, shouldn't ever happen)
                       AND o.close_date <= CURRENT_DATE
                       AND DATE_PART(dow, o.close_date) <> 0
                       -- Blacklist
                       AND o.user_id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'user_id' AND region_code = dataset.region_code)
                       AND o.id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'order_id' AND region_code = dataset.region_code)
                       AND op.product_id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'product_id' AND region_code = dataset.region_code)
                       -- KAM conditions
                       AND customers.region IS NOT NULL
                       AND dataset.warehouse <> 'ALL'
                       AND dataset.warehouse = customers.warehouse
            ),
            
            -------------------------------------------------
            -- Demand with KAMs removed 
            -------------------------------------------------
            demand AS
            (
            SELECT      orders.close_date AS order_close_date
            ,           COALESCE(ns.new_product_id, orders.order_product_id) AS demand_product_id
            ,           orders.dp_input_datasets_id AS demand_dp_input_datasets_id
            ,           orders.region_code AS region_code
            ,           orders.stockout_weight AS stockout_weight
            ,           SUM(orders.quantity * COALESCE(ns.old_units_per_new_unit, 1) ) AS quantity
            FROM        orders
            LEFT JOIN   postgres_main_{country}."reports.sandbox.new_skus" ns ON orders.order_product_id = ns.old_product_id
            GROUP BY    order_close_date, demand_product_id, demand_dp_input_datasets_id, region_code, stockout_weight
            ),
            
            -------------------------------------------------
            -- Stockouts
            -------------------------------------------------
            stockouts AS
            (
            SELECT      fs.close_date AS stockout_close_date
            ,           COALESCE(ns.new_product_id,fs.product_id) AS stockout_product_id
            ,           fs.region_code AS region_code
            ,           datasets_list.dataset_identifier AS dataset_identifier
            ,           SUM(fs.stockout_demand * COALESCE(ns.old_units_per_new_unit,  1) * datasets_list.stockout_weight) AS stockouts
            FROM        lnd_ops.dp_output_forecast_stockouts_by_wh fs
            INNER JOIN  datasets datasets_list ON datasets_list.region_code = fs.region_code AND datasets_list.warehouse = fs.warehouse
            INNER JOIN  dpr_shared.dim_site dt ON dt.identifier_value = fs.region_code
            INNER JOIN  dpr_shared.dim_country dc ON dc.country_id = dt.country_id
            LEFT JOIN   postgres_main_{country}."reports.sandbox.new_skus" ns ON fs.product_id = ns.old_product_id
            WHERE       fs.close_date <= CURRENT_DATE
                        AND LOWER(dc.identifier_value) = LOWER('{country}')
            GROUP BY    stockout_close_date, stockout_product_id, fs.region_code, dataset_identifier
            HAVING      stockouts > 0
            ),
            
            -------------------------------------------------
            -- Demand per day
            -------------------------------------------------
            demand_by_day AS
            (
            SELECT          COALESCE(demand.order_close_date, souts.stockout_close_date) AS date
            ,               COALESCE(demand.demand_product_id, souts.stockout_product_id) AS product_id
            ,               SUM(COALESCE(demand.quantity,0) + COALESCE(souts.stockouts, 0)) AS demand
            ,               COALESCE(demand.demand_dp_input_datasets_id, souts.dataset_identifier) AS dp_input_datasets_id
            FROM            demand 
            FULL OUTER JOIN stockouts souts 
                            ON souts.dataset_identifier = demand.demand_dp_input_datasets_id
                            AND souts.stockout_product_id * 1 = demand.demand_product_id * 1
                            AND DATE(souts.stockout_close_date) = DATE(demand.order_close_date)
            GROUP BY        date, product_id, dp_input_datasets_id

            ),
            
            -------------------------------------------------
            -- z-score calculation
            -------------------------------------------------
            z_score_day AS
            (
            SELECT          date,
                            product_id,
                            demand,
                            ISNULL((demand - AVG(demand) OVER(PARTITION BY product_id, dp_input_datasets_id))/NULLIF(stddev(demand) OVER (PARTITION BY product_id, dp_input_datasets_id),0),0) AS z_score,
                            dp_input_datasets_id
            FROM            demand_by_day
            )
            
            -------------------------------------------------
            -- Demand to ingest
            -------------------------------------------------
            
            SELECT          date,
                            product_id,
                            CASE 
                                WHEN z_score >= 2.81 OR z_score <= -2.81 THEN AVG(demand) OVER (PARTITION BY product_id, dp_input_datasets_id)
                                ELSE demand
                            END AS demand,
                            dp_input_datasets_id,
                            'experiment_5' AS experiment
            FROM            z_score_day
            {restriction}
            """
    return query

def exp6(country, dataset_id, restriction=""):
    if restriction:
        restriction = "WHERE " + restriction
    query = f"""
            WITH
            -------------------------------------------------
            -- List of datasets to build
            -------------------------------------------------
            datasets AS
            (
            SELECT      LOWER(dc.identifier_value) as country
            ,           ds.region_code as region_code
            ,           ds.warehouse AS warehouse
            ,           ds.id as dataset_identifier
            ,           ds.stockout_weight as stockout_weight
            FROM        postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" ds
            INNER JOIN  dpr_shared.dim_site dt ON dt.identifier_value = ds.region_code
            INNER JOIN  dpr_shared.dim_country dc ON dc.country_id = dt.country_id
            WHERE       ds.accumulated_days = 1
                        AND LOWER(dc.identifier_value) = LOWER('{country}')
                        AND ds.id = {dataset_id}
            ),
            
            -------------------------------------------------
            -- DAILY demand, full load, products in orders
            -------------------------------------------------
            orders AS
            (
            SELECT      o.close_date AS close_date
            ,           op.product_id AS order_product_id
            ,           op.quantity AS quantity
            ,           dataset.id AS dp_input_datasets_id
            ,           dataset.region_code AS region_code
            ,           dataset.stockout_weight AS stockout_weight
            FROM        postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" dataset
            INNER JOIN  postgres_main_{country}."reports.orders" o ON o.region_code = dataset.region_code
            INNER JOIN  postgres_main_{country}."reports.orders_products" op ON o.id = op.order_id
            INNER JOIN  postgres_main_federate."ops_demand_planning.forecast_context.dp_microzone_group" mgroup ON mgroup.microzone_group_code = dataset.microzone_group_code
            INNER JOIN  dpr_shared.dim_site region ON region.identifier_value = dataset.region_code
            INNER JOIN  dpr_shared.dim_microzone mzone ON mzone.site_id = region.site_id
                                                   AND mzone.microzone_id = mgroup.microzone_id
            INNER JOIN  datasets datasets_list
                                                    ON datasets_list.dataset_identifier = dataset.id
            LEFT JOIN   postgres_main_federate."ops_wh_planning.customer_configuration" customers ON customers.region = dataset.region_code 
                                                    AND customers.tag_name = o.tag_name 
                                                    AND customers.warehouse_vip = 'true'
            WHERE       -- Order Status
                       o.status = 1
                       AND op.is_deleted = 'false'
                       -- Remove future sales, and sundays (weird cases where close_date is on a sunday, shouldn't ever happen)
                       AND o.close_date <= CURRENT_DATE
                       AND DATE_PART(dow, o.close_date) <> 0
                       -- Blacklist
                       AND o.user_id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'user_id' AND region_code = dataset.region_code)
                       AND o.id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'order_id' AND region_code = dataset.region_code)
                       AND op.product_id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'product_id' AND region_code = dataset.region_code)
                       -- Microzone geometry contains the order location
                       AND (st_contains(mzone.boundaries, st_point(o.latitude, o.longitude))
                            AND (customers.warehouse IS NULL OR dataset.warehouse = 'ALL'))

            UNION ALL

            -- Filtros
            SELECT      o.close_date AS close_date
            ,           op.product_id AS order_product_id
            ,           op.quantity AS quantity
            ,           dataset.id AS dp_input_datasets_id
            ,           dataset.region_code AS region_code
            ,           dataset.stockout_weight AS stockout_weight
            FROM        postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" dataset
            INNER JOIN  postgres_main_{country}."reports.orders" o ON o.region_code = dataset.region_code
            INNER JOIN  postgres_main_{country}."reports.orders_products" op ON o.id = op.order_id
            INNER JOIN  datasets datasets_list
                                                    ON datasets_list.dataset_identifier = dataset.id
            INNER JOIN  postgres_main_federate."ops_wh_planning.customer_configuration" customers ON customers.region = dataset.region_code 
                                                    AND customers.tag_name = o.tag_name 
                                                    AND customers.warehouse_vip = 'true'
            WHERE       -- Order Status
                       o.status = 1
                       AND op.is_deleted = 'false'
                       -- Remove future sales, and sundays (weird cases where close_date is on a sunday, shouldn't ever happen)
                       AND o.close_date <= CURRENT_DATE
                       AND DATE_PART(dow, o.close_date) <> 0
                       -- Blacklist
                       AND o.user_id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'user_id' AND region_code = dataset.region_code)
                       AND o.id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'order_id' AND region_code = dataset.region_code)
                       AND op.product_id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'product_id' AND region_code = dataset.region_code)
                       -- KAM conditions
                       AND customers.region IS NOT NULL
                       AND dataset.warehouse <> 'ALL'
                       AND dataset.warehouse = customers.warehouse
            ),
            
            -------------------------------------------------
            -- Mean and Standard Deviation of Product Orders
            -------------------------------------------------
            deviation AS
            (
            SELECT      order_product_id AS deviation_product_id
            ,           dp_input_datasets_id AS dp_input_datasets_id
            ,           AVG(quantity) AS average
            ,           COALESCE(stddev(quantity), 0) AS stadDev
            FROM        orders products_demand
            GROUP BY    dp_input_datasets_id, deviation_product_id
            ),
            
            -------------------------------------------------
            -- Demand with outliers removed 
            -------------------------------------------------
            demand AS
            (
            SELECT      orders.close_date AS order_close_date
            ,           COALESCE(ns.new_product_id, orders.order_product_id) AS demand_product_id
            ,           orders.dp_input_datasets_id AS demand_dp_input_datasets_id
            ,           orders.region_code AS region_code
            ,           orders.stockout_weight AS stockout_weight
            ,           SUM(orders.quantity * COALESCE(ns.old_units_per_new_unit, 1) ) AS quantity
            FROM        orders
            LEFT JOIN   postgres_main_{country}."reports.sandbox.new_skus" ns ON orders.order_product_id = ns.old_product_id
            LEFT JOIN   deviation 
                                ON orders.dp_input_datasets_id = deviation.dp_input_datasets_id
                                AND orders.order_product_id = deviation.deviation_product_id
            WHERE       orders.quantity <= (deviation.average + deviation.stadDev * 6)
            GROUP BY    order_close_date, demand_product_id, demand_dp_input_datasets_id, region_code, stockout_weight
            ),
            
            -------------------------------------------------
            -- Stockouts
            -------------------------------------------------
            stockouts AS
            (
            SELECT      fs.close_date AS stockout_close_date
            ,           COALESCE(ns.new_product_id,fs.product_id) AS stockout_product_id
            ,           fs.region_code AS region_code
            ,           datasets_list.dataset_identifier AS dataset_identifier
            ,           SUM(fs.stockout_demand * COALESCE(ns.old_units_per_new_unit,  1) * datasets_list.stockout_weight) AS stockouts
            FROM        lnd_ops.dp_output_forecast_stockouts_by_wh fs
            INNER JOIN  datasets datasets_list ON datasets_list.region_code = fs.region_code AND datasets_list.warehouse = fs.warehouse
            INNER JOIN  dpr_shared.dim_site dt ON dt.identifier_value = fs.region_code
            INNER JOIN  dpr_shared.dim_country dc ON dc.country_id = dt.country_id
            LEFT JOIN   postgres_main_{country}."reports.sandbox.new_skus" ns ON fs.product_id = ns.old_product_id
            WHERE       fs.close_date <= CURRENT_DATE
                        AND LOWER(dc.identifier_value) = LOWER('{country}')
            GROUP BY    stockout_close_date, stockout_product_id, fs.region_code, dataset_identifier
            HAVING      stockouts > 0
            )
            
            -------------------------------------------------
            -- Demand to ingest
            -------------------------------------------------
            SELECT          COALESCE(demand.order_close_date, souts.stockout_close_date) AS date
            ,               COALESCE(demand.demand_product_id, souts.stockout_product_id) AS product_id
            ,               SUM(COALESCE(demand.quantity,0) + COALESCE(souts.stockouts, 0)) AS demand
            ,               COALESCE(demand.demand_dp_input_datasets_id, souts.dataset_identifier) AS dp_input_datasets_id
            ,               'experiment_6' AS experiment
            FROM            demand 
            FULL OUTER JOIN stockouts souts 
                            ON souts.dataset_identifier = demand.demand_dp_input_datasets_id
                            AND souts.stockout_product_id * 1 = demand.demand_product_id * 1
                            AND DATE(souts.stockout_close_date) = DATE(demand.order_close_date)
            {restriction}
            GROUP BY        date, product_id, dp_input_datasets_id
            """
    return query

def exp7(country, dataset_id, restriction=""):
    if restriction:
        restriction = "WHERE " + restriction
    query = f"""
            WITH
            -------------------------------------------------
            -- List of datasets to build
            -------------------------------------------------
            datasets AS
            (
            SELECT      LOWER(dc.identifier_value) as country
            ,           ds.region_code as region_code
            ,           ds.warehouse AS warehouse
            ,           ds.id as dataset_identifier
            ,           ds.stockout_weight as stockout_weight
            FROM        postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" ds
            INNER JOIN  dpr_shared.dim_site dt ON dt.identifier_value = ds.region_code
            INNER JOIN  dpr_shared.dim_country dc ON dc.country_id = dt.country_id
            WHERE       ds.accumulated_days = 1
                        AND LOWER(dc.identifier_value) = LOWER('{country}')
                        AND ds.id = {dataset_id}
            ),
            
            -------------------------------------------------
            -- DAILY demand, full load, products in orders
            -------------------------------------------------
            orders AS
            (
            SELECT      o.close_date AS close_date
            ,           op.product_id AS order_product_id
            ,           op.quantity AS quantity
            ,           dataset.id AS dp_input_datasets_id
            ,           dataset.region_code AS region_code
            ,           dataset.stockout_weight AS stockout_weight
            FROM        postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" dataset
            INNER JOIN  postgres_main_{country}."reports.orders" o ON o.region_code = dataset.region_code
            INNER JOIN  postgres_main_{country}."reports.orders_products" op ON o.id = op.order_id
            INNER JOIN  postgres_main_federate."ops_demand_planning.forecast_context.dp_microzone_group" mgroup ON mgroup.microzone_group_code = dataset.microzone_group_code
            INNER JOIN  dpr_shared.dim_site region ON region.identifier_value = dataset.region_code
            INNER JOIN  dpr_shared.dim_microzone mzone ON mzone.site_id = region.site_id
                                                   AND mzone.microzone_id = mgroup.microzone_id
            INNER JOIN  datasets datasets_list
                                                    ON datasets_list.dataset_identifier = dataset.id
            LEFT JOIN   postgres_main_federate."ops_wh_planning.customer_configuration" customers ON customers.region = dataset.region_code 
                                                    AND customers.tag_name = o.tag_name 
                                                    AND customers.warehouse_vip = 'true'
            WHERE       -- Order Status
                       o.status = 1
                       AND op.is_deleted = 'false'
                       -- Remove future sales, and sundays (weird cases where close_date is on a sunday, shouldn't ever happen)
                       AND o.close_date <= CURRENT_DATE
                       AND DATE_PART(dow, o.close_date) <> 0
                       -- Blacklist
                       AND o.user_id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'user_id' AND region_code = dataset.region_code)
                       AND o.id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'order_id' AND region_code = dataset.region_code)
                       AND op.product_id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'product_id' AND region_code = dataset.region_code)
                       -- Microzone geometry contains the order location
                       AND (st_contains(mzone.boundaries, st_point(o.latitude, o.longitude))
                            AND (customers.warehouse IS NULL OR dataset.warehouse = 'ALL'))

            UNION ALL

            -- Filtros
            SELECT      o.close_date AS close_date
            ,           op.product_id AS order_product_id
            ,           op.quantity AS quantity
            ,           dataset.id AS dp_input_datasets_id
            ,           dataset.region_code AS region_code
            ,           dataset.stockout_weight AS stockout_weight
            FROM        postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" dataset
            INNER JOIN  postgres_main_{country}."reports.orders" o ON o.region_code = dataset.region_code
            INNER JOIN  postgres_main_{country}."reports.orders_products" op ON o.id = op.order_id
            INNER JOIN  datasets datasets_list
                                                    ON datasets_list.dataset_identifier = dataset.id
            INNER JOIN  postgres_main_federate."ops_wh_planning.customer_configuration" customers ON customers.region = dataset.region_code 
                                                    AND customers.tag_name = o.tag_name 
                                                    AND customers.warehouse_vip = 'true'
            WHERE       -- Order Status
                       o.status = 1
                       AND op.is_deleted = 'false'
                       -- Remove future sales, and sundays (weird cases where close_date is on a sunday, shouldn't ever happen)
                       AND o.close_date <= CURRENT_DATE
                       AND DATE_PART(dow, o.close_date) <> 0
                       -- Blacklist
                       AND o.user_id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'user_id' AND region_code = dataset.region_code)
                       AND o.id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'order_id' AND region_code = dataset.region_code)
                       AND op.product_id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'product_id' AND region_code = dataset.region_code)
                       -- KAM conditions
                       AND customers.region IS NOT NULL
                       AND dataset.warehouse <> 'ALL'
                       AND dataset.warehouse = customers.warehouse
            ),
            
            -------------------------------------------------
            -- Percentile 99 of Product Orders
            -------------------------------------------------
            percentile AS
            (
            SELECT      order_product_id AS percentile_product_id
            ,           dp_input_datasets_id AS dp_input_datasets_id
            ,           PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY quantity) AS percentile_99
            FROM        orders products_demand
            GROUP BY    dp_input_datasets_id, percentile_product_id
            ),
                        
            -------------------------------------------------
            -- Demand with outliers removed by percentile orders
            -------------------------------------------------
            demand AS
            (
            SELECT      orders.close_date AS order_close_date
            ,           COALESCE(ns.new_product_id, orders.order_product_id) AS demand_product_id
            ,           orders.dp_input_datasets_id AS demand_dp_input_datasets_id
            ,           orders.region_code AS region_code
            ,           orders.stockout_weight AS stockout_weight
            ,           SUM(orders.quantity * COALESCE(ns.old_units_per_new_unit, 1) ) AS quantity
            FROM        orders
            LEFT JOIN   postgres_main_{country}."reports.sandbox.new_skus" ns ON orders.order_product_id = ns.old_product_id
            LEFT JOIN   percentile 
                                ON orders.dp_input_datasets_id = percentile.dp_input_datasets_id
                                AND orders.order_product_id = percentile.percentile_product_id
            WHERE       orders.quantity <= percentile.percentile_99
            GROUP BY    order_close_date, demand_product_id, demand_dp_input_datasets_id, region_code, stockout_weight
            ),
            
            -------------------------------------------------
            -- Stockouts
            -------------------------------------------------
            stockouts AS
            (
            SELECT      fs.close_date AS stockout_close_date
            ,           COALESCE(ns.new_product_id,fs.product_id) AS stockout_product_id
            ,           fs.region_code AS region_code
            ,           datasets_list.dataset_identifier AS dataset_identifier
            ,           SUM(fs.stockout_demand * COALESCE(ns.old_units_per_new_unit,  1) * datasets_list.stockout_weight) AS stockouts
            FROM        lnd_ops.dp_output_forecast_stockouts_by_wh fs
            INNER JOIN  datasets datasets_list ON datasets_list.region_code = fs.region_code AND datasets_list.warehouse = fs.warehouse
            INNER JOIN  dpr_shared.dim_site dt ON dt.identifier_value = fs.region_code
            INNER JOIN  dpr_shared.dim_country dc ON dc.country_id = dt.country_id
            LEFT JOIN   postgres_main_{country}."reports.sandbox.new_skus" ns ON fs.product_id = ns.old_product_id
            WHERE       fs.close_date <= CURRENT_DATE
                        AND LOWER(dc.identifier_value) = LOWER('{country}')
            GROUP BY    stockout_close_date, stockout_product_id, fs.region_code, dataset_identifier
            HAVING      stockouts > 0
            )
            
            -------------------------------------------------
            -- Demand to ingest
            -------------------------------------------------
            SELECT          COALESCE(demand.order_close_date, souts.stockout_close_date) AS date
            ,               COALESCE(demand.demand_product_id, souts.stockout_product_id) AS product_id
            ,               SUM(COALESCE(demand.quantity,0) + COALESCE(souts.stockouts, 0)) AS demand
            ,               COALESCE(demand.demand_dp_input_datasets_id, souts.dataset_identifier) AS dp_input_datasets_id
            ,               'experiment_7' AS experiment
            FROM            demand 
            FULL OUTER JOIN stockouts souts 
                            ON souts.dataset_identifier = demand.demand_dp_input_datasets_id
                            AND souts.stockout_product_id * 1 = demand.demand_product_id * 1
                            AND DATE(souts.stockout_close_date) = DATE(demand.order_close_date)
            {restriction}
            GROUP BY        date, product_id, dp_input_datasets_id
            """
    return query

def exp8(country, dataset_id, restriction=""):
    if restriction:
        restriction = "WHERE " + restriction
    query = f"""
            WITH
            -------------------------------------------------
            -- List of datasets to build
            -------------------------------------------------
            datasets AS
            (
            SELECT      LOWER(dc.identifier_value) as country
            ,           ds.region_code as region_code
            ,           ds.warehouse AS warehouse
            ,           ds.id as dataset_identifier
            ,           ds.stockout_weight as stockout_weight
            FROM        postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" ds
            INNER JOIN  dpr_shared.dim_site dt ON dt.identifier_value = ds.region_code
            INNER JOIN  dpr_shared.dim_country dc ON dc.country_id = dt.country_id
            WHERE       ds.accumulated_days = 1
                        AND LOWER(dc.identifier_value) = LOWER('{country}')
                        AND ds.id = {dataset_id}
            ),
            
            -------------------------------------------------
            -- DAILY demand, full load, products in orders
            -------------------------------------------------
            orders AS
            (
            SELECT      o.close_date AS close_date
            ,           op.product_id AS order_product_id
            ,           op.quantity AS quantity
            ,           dataset.id AS dp_input_datasets_id
            ,           dataset.region_code AS region_code
            ,           dataset.stockout_weight AS stockout_weight
            FROM        postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" dataset
            INNER JOIN  postgres_main_{country}."reports.orders" o ON o.region_code = dataset.region_code
            INNER JOIN  postgres_main_{country}."reports.orders_products" op ON o.id = op.order_id
            INNER JOIN  postgres_main_federate."ops_demand_planning.forecast_context.dp_microzone_group" mgroup ON mgroup.microzone_group_code = dataset.microzone_group_code
            INNER JOIN  dpr_shared.dim_site region ON region.identifier_value = dataset.region_code
            INNER JOIN  dpr_shared.dim_microzone mzone ON mzone.site_id = region.site_id
                                                   AND mzone.microzone_id = mgroup.microzone_id
            INNER JOIN  datasets datasets_list
                                                    ON datasets_list.dataset_identifier = dataset.id
            LEFT JOIN   postgres_main_federate."ops_wh_planning.customer_configuration" customers ON customers.region = dataset.region_code 
                                                    AND customers.tag_name = o.tag_name 
                                                    AND customers.warehouse_vip = 'true'
            WHERE       -- Order Status
                       o.status = 1
                       AND op.is_deleted = 'false'
                       -- Remove future sales, and sundays (weird cases where close_date is on a sunday, shouldn't ever happen)
                       AND o.close_date <= CURRENT_DATE
                       AND DATE_PART(dow, o.close_date) <> 0
                       -- Blacklist
                       AND o.user_id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'user_id' AND region_code = dataset.region_code)
                       AND o.id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'order_id' AND region_code = dataset.region_code)
                       AND op.product_id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'product_id' AND region_code = dataset.region_code)
                       -- Microzone geometry contains the order location
                       AND (st_contains(mzone.boundaries, st_point(o.latitude, o.longitude))
                            AND (customers.warehouse IS NULL OR dataset.warehouse = 'ALL'))

            UNION ALL

            -- Filtros
            SELECT      o.close_date AS close_date
            ,           op.product_id AS order_product_id
            ,           op.quantity AS quantity
            ,           dataset.id AS dp_input_datasets_id
            ,           dataset.region_code AS region_code
            ,           dataset.stockout_weight AS stockout_weight
            FROM        postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" dataset
            INNER JOIN  postgres_main_{country}."reports.orders" o ON o.region_code = dataset.region_code
            INNER JOIN  postgres_main_{country}."reports.orders_products" op ON o.id = op.order_id
            INNER JOIN  datasets datasets_list
                                                    ON datasets_list.dataset_identifier = dataset.id
            INNER JOIN  postgres_main_federate."ops_wh_planning.customer_configuration" customers ON customers.region = dataset.region_code 
                                                    AND customers.tag_name = o.tag_name 
                                                    AND customers.warehouse_vip = 'true'
            WHERE       -- Order Status
                       o.status = 1
                       AND op.is_deleted = 'false'
                       -- Remove future sales, and sundays (weird cases where close_date is on a sunday, shouldn't ever happen)
                       AND o.close_date <= CURRENT_DATE
                       AND DATE_PART(dow, o.close_date) <> 0
                       -- Blacklist
                       AND o.user_id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'user_id' AND region_code = dataset.region_code)
                       AND o.id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'order_id' AND region_code = dataset.region_code)
                       AND op.product_id NOT IN (SELECT element_id FROM lnd_ops.dp_queries_black_list WHERE type = 'product_id' AND region_code = dataset.region_code)
                       -- KAM conditions
                       AND customers.region IS NOT NULL
                       AND dataset.warehouse <> 'ALL'
                       AND dataset.warehouse = customers.warehouse
            ),
            
            -------------------------------------------------
            -- Mean and Standard Deviation of Product Orders
            -------------------------------------------------
            deviation AS
            (
            SELECT      order_product_id AS deviation_product_id
            ,           dp_input_datasets_id AS dp_input_datasets_id
            ,           AVG(quantity) AS average
            ,           COALESCE(stddev(quantity), 0) AS stadDev
            FROM        orders products_demand
            GROUP BY    dp_input_datasets_id, deviation_product_id
            ),
            
            -------------------------------------------------
            -- Percentile 99 of Product Orders
            -------------------------------------------------
            percentile AS
            (
            SELECT      order_product_id AS percentile_product_id
            ,           dp_input_datasets_id AS dp_input_datasets_id
            ,           PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY quantity) AS percentile_99
            FROM        orders products_demand
            GROUP BY    dp_input_datasets_id, percentile_product_id
            ),
                        
            -------------------------------------------------
            -- Demand with outliers removed by percentile orders and 6 stddev
            -------------------------------------------------
            demand AS
            (
            SELECT      orders.close_date AS order_close_date
            ,           COALESCE(ns.new_product_id, orders.order_product_id) AS demand_product_id
            ,           orders.dp_input_datasets_id AS demand_dp_input_datasets_id
            ,           orders.region_code AS region_code
            ,           orders.stockout_weight AS stockout_weight
            ,           SUM(orders.quantity * COALESCE(ns.old_units_per_new_unit, 1) ) AS quantity
            FROM        orders
            LEFT JOIN   postgres_main_{country}."reports.sandbox.new_skus" ns ON orders.order_product_id = ns.old_product_id
            LEFT JOIN   deviation 
                                ON orders.dp_input_datasets_id = deviation.dp_input_datasets_id
                                AND orders.order_product_id = deviation.deviation_product_id
            LEFT JOIN   percentile 
                                ON orders.dp_input_datasets_id = percentile.dp_input_datasets_id
                                AND orders.order_product_id = percentile.percentile_product_id
            WHERE       orders.quantity <= (deviation.average + deviation.stadDev * 6)
                AND     orders.quantity <= percentile.percentile_99
            GROUP BY    order_close_date, demand_product_id, demand_dp_input_datasets_id, region_code, stockout_weight
            ),
            
            -------------------------------------------------
            -- Stockouts
            -------------------------------------------------
            stockouts AS
            (
            SELECT      fs.close_date AS stockout_close_date
            ,           COALESCE(ns.new_product_id,fs.product_id) AS stockout_product_id
            ,           fs.region_code AS region_code
            ,           datasets_list.dataset_identifier AS dataset_identifier
            ,           SUM(fs.stockout_demand * COALESCE(ns.old_units_per_new_unit,  1) * datasets_list.stockout_weight) AS stockouts
            FROM        lnd_ops.dp_output_forecast_stockouts_by_wh fs
            INNER JOIN  datasets datasets_list ON datasets_list.region_code = fs.region_code AND datasets_list.warehouse = fs.warehouse
            INNER JOIN  dpr_shared.dim_site dt ON dt.identifier_value = fs.region_code
            INNER JOIN  dpr_shared.dim_country dc ON dc.country_id = dt.country_id
            LEFT JOIN   postgres_main_{country}."reports.sandbox.new_skus" ns ON fs.product_id = ns.old_product_id
            WHERE       fs.close_date <= CURRENT_DATE
                        AND LOWER(dc.identifier_value) = LOWER('{country}')
            GROUP BY    stockout_close_date, stockout_product_id, fs.region_code, dataset_identifier
            HAVING      stockouts > 0--;
            ),
            
            -------------------------------------------------
            -- Demand per day
            -------------------------------------------------
            demand_by_day AS
            (
            SELECT          COALESCE(demand.order_close_date, souts.stockout_close_date) AS date
            ,               COALESCE(demand.demand_product_id, souts.stockout_product_id) AS product_id
            ,               SUM(COALESCE(demand.quantity,0) + COALESCE(souts.stockouts, 0)) AS demand
            ,               COALESCE(demand.demand_dp_input_datasets_id, souts.dataset_identifier) AS dp_input_datasets_id
            FROM            demand 
            FULL OUTER JOIN stockouts souts 
                            ON souts.dataset_identifier = demand.demand_dp_input_datasets_id
                            AND souts.stockout_product_id * 1 = demand.demand_product_id * 1
                            AND DATE(souts.stockout_close_date) = DATE(demand.order_close_date)
            GROUP BY        date, product_id, dp_input_datasets_id

            ),
            
            -------------------------------------------------
            -- z-score calculation
            -------------------------------------------------
            z_score_day AS
            (
            SELECT          date,
                            product_id,
                            demand,
                            ISNULL((demand - AVG(demand) OVER(PARTITION BY product_id, dp_input_datasets_id))/NULLIF(stddev(demand) OVER (PARTITION BY product_id, dp_input_datasets_id),0),0) AS z_score,
                            dp_input_datasets_id
            FROM            demand_by_day
            )
            
            -------------------------------------------------
            -- Demand to ingest
            -------------------------------------------------
            
            SELECT          date,
                            product_id,
                            CASE 
                                WHEN z_score >= 2.81 OR z_score <= -2.81 THEN AVG(demand) OVER (PARTITION BY product_id, dp_input_datasets_id)
                                ELSE demand
                            END AS demand,
                            dp_input_datasets_id,
                            'experiment_8' AS experiment
            FROM            z_score_day
            {restriction}
            """
    return query
