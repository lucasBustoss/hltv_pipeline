WITH players AS (
    SELECT DISTINCT
        player_id,
        team_id, 
        nick AS player_nick
    FROM {{ source('hltv', 'players') }}
)

SELECT * FROM players