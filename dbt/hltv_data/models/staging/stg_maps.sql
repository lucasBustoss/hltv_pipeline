WITH maps AS (
    SELECT DISTINCT
        id AS map_id,
        name AS map_name
    FROM {{ source('hltv', 'maps') }}
)

SELECT * FROM maps