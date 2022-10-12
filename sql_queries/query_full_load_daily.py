def query_full_load_daily():
    query = f"""
            begin transaction;

            lock lnd_ops.dp_input_demand;

            -------------------------------------------------
            -- List of datasets to build
            -------------------------------------------------

            create temporary table lnd_ops_dev.{params.table_datasets_list}_{params.country} (country, region_code, dataset_identifier, stockout_weight) AS
            SELECT      LOWER(dc.identifier_value) as country
            ,           ds.region_code as region_code
            ,           ds.id as dataset_identifier
            ,           ds.stockout_weight as stockout_weight
            FROM        postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" ds
            INNER JOIN  dpr_shared.dim_site dt ON dt.identifier_value = ds.region_code
            INNER JOIN  dpr_shared.dim_country dc ON dc.country_id = dt.country_id
            WHERE       ds.accumulated_days = 1
                        AND LOWER(dc.identifier_value) = LOWER('{params.country}');

            -------------------------------------------------
            -- DAILY demand, full load, products in orders
            -------------------------------------------------
            create temporary table {params.target_schema}.{params.table_order_products}_{params.country} (close_date, order_product_id, quantity, dp_input_datasets_id, region_code, stockout_weight) as
            SELECT      o.close_date AS close_date
            ,           op.product_id AS order_product_id
            ,           op.quantity AS quantity
            ,           dataset.id AS dp_input_datasets_id
            ,           dataset.region_code AS region_code
            ,           dataset.stockout_weight AS stockout_weight
            FROM        postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" dataset
            INNER JOIN  postgres_main_{params.country}."reports.orders" o ON o.region_code = dataset.region_code
            INNER JOIN  postgres_main_{params.country}."reports.orders_products" op ON o.id = op.order_id
            INNER JOIN  postgres_main_federate."ops_demand_planning.forecast_context.dp_microzone_group" mgroup ON mgroup.microzone_group_code = dataset.microzone_group_code
            INNER JOIN  dpr_shared.dim_site region ON region.identifier_value = dataset.region_code
            INNER JOIN  dpr_shared.dim_microzone mzone ON mzone.site_id = region.site_id
                                                   AND mzone.microzone_id = mgroup.microzone_id
            INNER JOIN  {params.target_schema}.{params.table_datasets_list}_{params.country} datasets_list
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
                       AND o.user_id NOT IN (SELECT element_id FROM {params.target_schema}.dp_queries_black_list WHERE type = 'user_id' AND region_code = dataset.region_code)
                       AND o.id NOT IN (SELECT element_id FROM {params.target_schema}.dp_queries_black_list WHERE type = 'order_id' AND region_code = dataset.region_code)
                       AND op.product_id NOT IN (SELECT element_id FROM {params.target_schema}.dp_queries_black_list WHERE type = 'product_id' AND region_code = dataset.region_code)
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
            INNER JOIN  postgres_main_{params.country}."reports.orders" o ON o.region_code = dataset.region_code
            INNER JOIN  postgres_main_{params.country}."reports.orders_products" op ON o.id = op.order_id
            INNER JOIN  {params.target_schema}.{params.table_datasets_list}_{params.country} datasets_list
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
                       AND o.user_id NOT IN (SELECT element_id FROM {params.target_schema}.dp_queries_black_list WHERE type = 'user_id' AND region_code = dataset.region_code)
                       AND o.id NOT IN (SELECT element_id FROM {params.target_schema}.dp_queries_black_list WHERE type = 'order_id' AND region_code = dataset.region_code)
                       AND op.product_id NOT IN (SELECT element_id FROM {params.target_schema}.dp_queries_black_list WHERE type = 'product_id' AND region_code = dataset.region_code)
                       -- KAM conditions
                       AND customers.region IS NOT NULL
                       AND dataset.warehouse <> 'ALL'
                       AND dataset.warehouse = customers.warehouse;
            
            
            -------------------------------------------------
            -- Mean and Standard Deviation of Product Orders
            -------------------------------------------------
            create table {params.target_schema}.{params.table_product_demand_stats}_{params.country} (deviation_product_id, dp_input_datasets_id, average, stadDev) as
            SELECT      order_product_id AS deviation_product_id
            ,           dp_input_datasets_id AS dp_input_datasets_id
            ,           AVG(quantity) AS average
            ,           COALESCE(stddev(quantity), 0) AS stadDev
            FROM        {params.target_schema}.{params.table_order_products}_{params.country} products_demand
            GROUP BY    dp_input_datasets_id, deviation_product_id;

            
            -------------------------------------------------
            -- Demand with outliers removed 
            -------------------------------------------------
            create table {params.target_schema}.{params.table_product_clean_demand}_{params.country} (order_close_date, demand_product_id, demand_dp_input_datasets_id, region_code, stockout_weight, quantity) as
            SELECT      orders.close_date AS order_close_date
            ,           COALESCE(ns.new_product_id, orders.order_product_id) AS demand_product_id
            ,           orders.dp_input_datasets_id AS demand_dp_input_datasets_id
            ,           orders.region_code AS region_code
            ,           orders.stockout_weight AS stockout_weight
            ,           SUM(orders.quantity * COALESCE(ns.old_units_per_new_unit, 1) ) AS quantity
            FROM        {params.target_schema}.{params.table_order_products}_{params.country} orders
            LEFT JOIN   postgres_main_{params.country}."reports.sandbox.new_skus" ns ON orders.order_product_id = ns.old_product_id
            LEFT JOIN   {params.target_schema}.{params.table_product_demand_stats}_{params.country} deviation 
                                            ON orders.dp_input_datasets_id = deviation.dp_input_datasets_id
                                            AND orders.order_product_id = deviation.deviation_product_id
            WHERE       orders.quantity <= (deviation.average + deviation.stadDev * 6)
            GROUP BY    order_close_date, demand_product_id, demand_dp_input_datasets_id, region_code, stockout_weight;

            
            -------------------------------------------------
            -- Stockouts
            -------------------------------------------------
            create temporary table {params.target_schema}.{params.table_stockouts}_{params.country} (stockout_close_date, stockout_product_id, region_code, dataset_identifier, stockouts) as
            SELECT      fs.close_date AS stockout_close_date
            ,           COALESCE(ns.new_product_id,fs.product_id) AS stockout_product_id
            ,           fs.region_code AS region_code
            ,           datasets_list.dataset_identifier AS dataset_identifier
            ,           SUM(fs.stockout_demand * COALESCE(ns.old_units_per_new_unit,  1) * datasets_list.stockout_weight) AS stockouts
            FROM        {params.target_schema}.dp_output_forecast_stockouts fs
            INNER JOIN  {params.target_schema}.{params.table_datasets_list}_{params.country} datasets_list ON datasets_list.region_code = fs.region_code
            INNER JOIN  dpr_shared.dim_site dt ON dt.identifier_value = fs.region_code
            INNER JOIN  dpr_shared.dim_country dc ON dc.country_id = dt.country_id
            LEFT JOIN   postgres_main_{params.country}."reports.sandbox.new_skus" ns ON fs.product_id = ns.old_product_id
            WHERE       fs.close_date <= CURRENT_DATE
                        AND LOWER(dc.identifier_value) = LOWER('{params.country}')
            GROUP BY    stockout_close_date, stockout_product_id, fs.region_code, dataset_identifier
            HAVING      stockouts > 0;
            

            -------------------------------------------------
            -- Demand to ingest
            -------------------------------------------------
            INSERT INTO {params.target_schema}.{params.table_input_demand}(date, product_id, demand, dp_input_datasets_id)
            SELECT          COALESCE(demand.order_close_date, souts.stockout_close_date) AS date
            ,               COALESCE(demand.demand_product_id, souts.stockout_product_id) AS product_id
            ,               SUM(COALESCE(demand.quantity,0) + COALESCE(souts.stockouts, 0)) AS demand
            ,               COALESCE(demand.demand_dp_input_datasets_id, souts.dataset_identifier) AS dp_input_datasets_id
            FROM            {params.target_schema}.{params.table_product_clean_demand}_{params.country} demand 
            FULL OUTER JOIN {params.target_schema}.{params.table_stockouts}_{params.country} souts 
                                                    ON souts.dataset_identifier = demand.demand_dp_input_datasets_id
                                                    AND souts.stockout_product_id * 1 = demand.demand_product_id * 1
                                                    AND DATE(souts.stockout_close_date) = DATE(demand.order_close_date)
            GROUP BY        date, product_id, dp_input_datasets_id;

            commit transaction;
            """