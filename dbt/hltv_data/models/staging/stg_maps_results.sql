WITH 
    maps_teams_ids AS (
        SELECT 
            match_id, 
            MIN(t.team_id) team1_id, 
            MAX(t.team_id) team2_id  
        FROM {{ source('hltv', 'maps_results') }} mr 
        INNER JOIN {{ source('hltv', 'teams') }} t
            ON t."name" = mr.team_name  
        GROUP BY 1
    ),
    maps_teams AS (
        SELECT DISTINCT
            match_id,
            t1.team_id team1_id,
            t1.name team1_name,
            t2.team_id team2_id,
            t2.name team2_name
        FROM maps_teams_ids mt
        INNER JOIN {{ source('hltv', 'teams') }} t1
            ON mt.team1_id = t1.team_id
        INNER JOIN {{ source('hltv', 'teams') }} t2 
            ON mt.team2_id = t2.team_id 
    ),
    maps_results_final AS (
        SELECT DISTINCT
            mr_team1.match_id,
            CASE
                WHEN (TRIM(m.best_of_type) = 'Best of 1' and (mr_team1.choice_number = '5' OR mr_team1.choice_number = '7')) OR (mr_team1.choice_number = '3') THEN 'Map 1'
                WHEN (TRIM(m.best_of_type) = 'Best of 1' and mr_team1.choice_number = '6') OR (mr_team1.choice_number = '4') THEN 'Map 2'
                WHEN (TRIM(m.best_of_type) = 'Best of 3' and mr_team1.choice_number = '7') OR (TRIM(m.best_of_type) = 'Best of 5' AND mr_team1.choice_number = '5') THEN 'Map 3'
                WHEN (TRIM(m.best_of_type) = 'Best of 5' and mr_team1.choice_number = '6') THEN 'Map 4'
                WHEN (TRIM(m.best_of_type) = 'Best of 5' and mr_team1.choice_number = '7') THEN 'Map 5'
            END AS "map_choice",
            mr_team1.choice_number,
            mr_team1.map_name,
            COALESCE(mr_team1.pick_team_id, 'Left') pick_team_id,
            mr_team1.team_name AS team1_id,
            mr_team1."result" AS team1_result,
            mr_team1.ct AS team1_ct,
            mr_team1.t AS team1_t,
            mr_team2.team_name AS team2_id
        FROM {{ source('hltv', 'maps_results') }} mr_team1
        INNER JOIN {{ source('hltv', 'matchs') }} m 
            ON mr_team1.match_id = m.match_id 
        INNER JOIN {{ source('hltv', 'maps_results') }} mr_team2 
            ON 	mr_team1.match_id = mr_team2.match_id 
            AND mr_team1.choice_number = mr_team2.choice_number 
            AND mr_team1.team_name != mr_team2.team_name 
        INNER JOIN maps_teams mt
            ON mt.team1_name = mr_team1.team_name 
            AND mt.team2_name = mr_team2.team_name 
        ORDER BY 1, 2
    )

SELECT * FROM maps_results_final
