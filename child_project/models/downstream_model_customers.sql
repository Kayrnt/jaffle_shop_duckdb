MODEL (
  name dbt_cou.downstream_model_customers
);

SELECT DISTINCT customer_id
FROM dbt_cou.customer_id