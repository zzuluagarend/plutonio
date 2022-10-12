def query_full_load():
    query = f"""
            begin transaction;

            ----------------------------------------------------------
            -- All the dates
            ----------------------------------------------------------
            create table {{params.target_schema}}.{{params.table_all_dates}}_{{params.country}} (raw_date) AS
            SELECT     DISTINCT date AS raw_date
            FROM       {{params.target_schema}}.{{params.table_input_demand}}
            WHERE      DATE_PART(dow, raw_date) <> 0;   -- Remove Sundays

            alter table {{params.target_schema}}.{{params.table_all_dates}}_{{params.country}} owner to usr_be_demand_planning;
            grant all privileges on {{params.target_schema}}.{{params.table_all_dates}}_{{params.country}} to usr_be_demand_planning;
            grant select, references on {{params.target_schema}}.{{params.table_all_dates}}_{{params.country}} to usr_ro_demand_planning;
            grant select, references on {{params.target_schema}}.{{params.table_all_dates}}_{{params.country}} to read_only;
            grant select, references on {{params.target_schema}}.{{params.table_all_dates}}_{{params.country}} to dw_read_only;


            ----------------------------------------------------------
            -- Demand start date per dataset and product, for the 
            -- specified country
            ----------------------------------------------------------
            create table {{params.target_schema}}.{{params.table_product_demand_dates}}_{{params.country}} (dp_input_datasets_id, product_id, start_demand_date) AS
            SELECT     demand.dp_input_datasets_id AS dp_input_datasets_id
            ,          demand.product_id AS product_id
            ,          MIN(demand.date) AS start_demand_date
            FROM       dpr_shared.dim_country country
            INNER JOIN dpr_shared.dim_site region ON region.country_id = country.country_id
            INNER JOIN postgres_main_federate."ops_demand_planning.forecast_context.dp_input_datasets_identifiers" dataset
                                  ON dataset.region_code = region.identifier_value
                                  AND dataset.accumulated_days = 1
            INNER JOIN {{params.target_schema}}.{{params.table_input_demand}} demand ON demand.dp_input_datasets_id = dataset.id
            WHERE      LOWER(country.identifier_value) = LOWER('{{params.country}}')
            GROUP BY dp_input_datasets_id, product_id;

            alter table {{params.target_schema}}.{{params.table_product_demand_dates}}_{{params.country}} owner to usr_be_demand_planning;
            grant all privileges on {{params.target_schema}}.{{params.table_product_demand_dates}}_{{params.country}} to usr_be_demand_planning;
            grant select, references on {{params.target_schema}}.{{params.table_product_demand_dates}}_{{params.country}} to usr_ro_demand_planning;
            grant select, references on {{params.target_schema}}.{{params.table_product_demand_dates}}_{{params.country}} to read_only;
            grant select, references on {{params.target_schema}}.{{params.table_product_demand_dates}}_{{params.country}} to dw_read_only;

            commit transaction;
            """