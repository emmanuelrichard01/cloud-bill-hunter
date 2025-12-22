SELECT DISTINCT
    "LineItem/ResourceId" as resource_id,
    "LineItem/ProductCode" as service,
    COALESCE("ResourceTags/user:Owner", 'Unknown') as owner_team
FROM bronze_billing;