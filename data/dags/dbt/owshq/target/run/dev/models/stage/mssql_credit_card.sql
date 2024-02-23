
  
    

    create table "iceberg"."landing"."mssql_credit_card__dbt_tmp"
      
      
    as (
      

SELECT
    DISTINCT CAST(
        id AS INT
    ) AS credit_card_id,
    CAST(
        user_id AS INT
    ) AS user_id,
    UID AS tsn_uid,
    credit_card_number AS credit_card_number,
    CAST(
        credit_card_expiry_date AS DATE
    ) AS credit_card_expiry,
    credit_card_type AS credit_card_type,
    CAST(
        dt_current_timestamp AS TIMESTAMP
    ) AS last_updated
FROM
    "minio"."landing"."mssql_credit_card_parquet"
    );

  