WITH match_stats AS (
    SELECT 
        ms_total.match_id,
        ms_total.map_name,
        ms_total.nick,
        ms_total.player_id,
        ms_total.team_id,
        ms_total.adr AS "adr_total",
        ms_total.diff AS "diff_total",
        ms_total.kast AS "kast_total",
        ms_total.kd AS "kd_total",
        ms_total.rating AS "rating_total",
        ms_ct.adr AS "adr_ct",
        ms_ct.diff AS "diff_ct",
        ms_ct.kast AS "kast_ct",
        ms_ct.kd AS "kd_ct",
        ms_ct.rating AS "rating_ct",
        ms_tr.adr AS "adr_tr",
        ms_tr.diff AS "diff_tr",
        ms_tr.kast AS "kast_tr",
        ms_tr.kd AS "kd_tr",
        ms_tr.rating AS "rating_tr"
    FROM {{ source('hltv', 'match_stats') }}  ms_total
    INNER JOIN {{ source('hltv', 'match_stats') }} ms_ct
        ON ms_ct.side = 'ct'
        AND ms_ct.match_id = ms_total.match_id 
        AND ms_ct.map_name = ms_total.map_name 
        AND ms_ct.player_id = ms_total.player_id 
        AND ms_ct.team_id = ms_total.team_id 
    INNER JOIN {{ source('hltv', 'match_stats') }} ms_tr
        ON ms_tr.side = 'tr'
        AND ms_tr.match_id = ms_total.match_id 
        AND ms_tr.map_name = ms_total.map_name 
        AND ms_tr.player_id = ms_total.player_id 
        AND ms_tr.team_id = ms_total.team_id 
    WHERE 
        ms_total.side = 'total'
    ORDER BY map_name 
)

SELECT * FROM match_stats
