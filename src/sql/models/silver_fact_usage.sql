SELECT
    "LineItem/ResourceId" as resource_id,
    CAST("LineItem/UsageStartDate" AS DATE) as usage_date,
    CAST("LineItem/UnblendedCost" AS DOUBLE) as cost,
    CAST("LineItem/UsageAmount" AS DOUBLE) as usage_amount
FROM bronze_billing
WHERE "LineItem/ResourceId" IS NOT NULL;