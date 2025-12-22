SELECT 
    "LineItem/ResourceId" as resource_id,
    "LineItem/ProductCode" as service,
    "ResourceTags/user:Owner" as owner,
    SUM("LineItem/UnblendedCost") as wasted_cost,
    SUM("LineItem/UsageAmount") as total_usage
FROM billing
GROUP BY 1, 2, 3
HAVING 
    SUM("LineItem/UnblendedCost") > 0 
    AND SUM("LineItem/UsageAmount") = 0
ORDER BY wasted_cost DESC;