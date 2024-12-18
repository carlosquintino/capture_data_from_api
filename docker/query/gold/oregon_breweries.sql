SELECT
    id AS brewery_id,
    name AS brewery_name,
    brewery_type,
    CONCAT(street, ', ', city, ', ', state_province, ', ', postal_code) AS location,
    country,
    CONCAT(latitude, ', ', longitude) AS coordinates,
    phone,
    website_url,
    state_province AS state
FROM
    silver_brewery.breweries
WHERE
    brewery_type IS NOT NULL
    AND state = 'Oregon';
