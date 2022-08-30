WITH teams AS (
    SELECT DISTINCT
        team_id,
        name AS team_name, 
        link AS team_link
    FROM {{ source('hltv', 'teams') }}
)

SELECT * FROM teams