WITH maps_vetos AS (
    SELECT DISTINCT
        match_id,
        type,
        choice_number,
        m.id AS map_id,
        COALESCE(team_id, 'Left') AS pick_team_id 
    FROM {{ source('hltv', 'maps_vetos') }} mv
    INNER JOIN {{ source('hltv', 'maps') }} m
        ON mv.map_name = m.name
)

SELECT * FROM maps_vetos