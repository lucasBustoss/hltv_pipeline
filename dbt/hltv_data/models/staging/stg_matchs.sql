WITH matchs AS (
    SELECT 
        match_id,
        "type",
        date,
        CASE
            WHEN TRIM(best_of_type) = 'Best of 3' THEN 'MD3'
            WHEN TRIM(best_of_type) = 'Best of 5' THEN 'MD5'
            ELSE 'MD1'
        END AS "best_of_type",
        team1_id,
        team2_id,
        CASE
            WHEN TRIM(best_of_type) = 'Best of 1' THEN 
            CASE
                WHEN team1_score::INTEGER > team2_score::INTEGER THEN 1
                ELSE 0
            end
            ELSE team1_score::INTEGER
        END AS "team1_score",
        CASE
            when TRIM(best_of_type) = 'Best of 1' THEN 
            CASE
                WHEN team2_score::INTEGER > team1_score::INTEGER THEN 1
                ELSE 0
            END
            ELSE team2_score::INTEGER
        END AS "team2_score"
    FROM {{ source('hltv', 'matchs') }} 
)

SELECT * FROM matchs