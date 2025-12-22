SELECT
    d.resource_id,
    d.service,
    d.owner_team,
    SUM(f.cost) as total_wasted_cost
FROM silver_fact_usage f
    JOIN silver_dim_resource d ON f.resource_id = d.resource_id
GROUP BY 1, 2, 3
HAVING 
    SUM(f.cost) > 0
    AND SUM(f.usage_amount) = 0
ORDER BY total_wasted_cost DESC;