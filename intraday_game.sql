select 
  * except (opp_user_won,forfeit_flag,minimize_flag),
  case
    when opponent_type in ('Bot') then -1
    else opp_user_won
  end as opp_user_won,
  case
    when opponent_type in ('Bot') and user_won is not null and min_user_round < 3 then 1
    when opponent_type in ('Bot') and user_won is not null and max_user_round >= 3 and max_user_round <=5  and ((5-max_user_round) >= abs(user_score - opp_score)) then 1
    when opponent_type in ('Real') and user_won is not null and opp_user_won is not null and min_user_round < 3 then 1
    when opponent_type in ('Real') and user_won is not null and opp_user_won is not null and max_user_round >= 3 and max_user_round <=5  and ((5-max_user_round) >= abs(user_score - opp_score)) then 1
    else 0
  end as issue_flag,

  case
    when opponent_type in ('Bot') and user_won is null then 0
    when opponent_type in ('Bot') and user_won is not null and (user_score = coalesce(user_score_calculated,0)) and (opp_score = coalesce(opp_score_calculated,0)) then 0
    when opponent_type in ('Real') and ((user_won is null) or (opp_user_won is null)) then 0
    -- when opponent_type in ('Real') and user_won is not null and opp_user_won is not null and ((user_score = 0 and user_score_calculated is null) or (user_score = 0 and opp_opp_score_calculated is null)) and ((opp_score = 0 and opp_score_calculated is null) or (opp_score = 0 and opp_user_score_calculated is null)) then 0
    when opponent_type in ('Real') and user_won is not null and opp_user_won is not null and ((user_score = coalesce(user_score_calculated,0)) or (user_score = coalesce(opp_opp_score_calculated,0))) and ((opp_score = coalesce(opp_score_calculated,0)) or (opp_score = coalesce(opp_user_score_calculated,0))) then 0
    else 1
  end as wrong_result_flag,

  case
    when forfeit_flag is null then 0
    else forfeit_flag
  end as forfeit_flag,

  case
    when minimize_flag is null then 0
    else minimize_flag
  end as minimize_flag,


from 
  (select
    a.event_date,
    a.event_timestamp,
    a.date,
    a.hour,
    a.app_version,
    a.room_name,
    a.user_pseudo_id,
    b.user_pseudo_id as opp_id,
    a.country,
    b.country as opp_country,
    a.opponent_type,
    a.user_won,
    a.user_score,
    a.opp_score,
    a.kicked_array,
    a.defend_array,
    a.user_score_calculated,
    a.opp_score_calculated,
    a.max_user_round,
    a.min_user_round,
    a.forfeit_flag,
    a.minimize_flag,

    b.user_won as opp_user_won,
    b.user_score as opp_user_score,
    b.opp_score as opp_opp_score,
    b.kicked_array as opp_kicked_array,
    b.defend_array as opp_defend_array,
    b.user_score_calculated as opp_user_score_calculated,
    b.opp_score_calculated as opp_opp_score_calculated,
    b.max_opp_round,
    b.min_opp_round,
    case
      when a.opponent_type in ('Bot') then 0
      when a.opponent_type in ('Real') and a.kicked_array is null and b.defend_array is null then 0
      when a.opponent_type in ('Real') and (a.kicked_array = b.defend_array) then 0
      else 1
    end as kicked_flag,

    case
      when a.opponent_type in ('Bot') then 0
      when a.opponent_type in ('Real') and a.defend_array is null and b.kicked_array is null then 0
      when a.opponent_type in ('Real') and (a.defend_array = b.kicked_array) then 0
      else 1
    end as defend_flag,

    case
      when a.opponent_type in ('Bot') then 0
      when a.opponent_type in ('Real') and (a.min_user_round = b.min_opp_round) then 0
      else 1
    end as round_flag,

    -- case
    --   when a.opponent_type in ('Bot') then 0
    --   when a.opponent_type in ('Real') and a.user_score is null and b.opp_score is null then 0
    --   when a.opponent_type in ('Real') and (a.user_score = b.opp_score) then 0
    --   else 1
    -- end as user_score_flag,

    -- case
    --   when a.opponent_type in ('Bot') then 0
    --   when a.opponent_type in ('Real') and a.opp_score is null and b.user_score is null then 0
    --   when a.opponent_type in ('Real') and (a.opp_score = b.user_score) then 0
    --   else 1
    -- end as opp_score_flag
    
  from 

    (select distinct 
      event_date,
      event_timestamp,
      date,
      hour,
      country,
      app_version,
      user_pseudo_id,
      room_name,
      opponent_type,
      user_won,
      user_score,
      opp_score,
      regexp_replace(kicked_array, r'[, "\]\[]', '') as kicked_array,
      regexp_replace(defend_array, r'[, "\]\[]', '') as defend_array,
      ARRAY_LENGTH(REGEXP_EXTRACT_ALL(kicked_array, r'(G)')) as user_score_calculated,
      ARRAY_LENGTH(REGEXP_EXTRACT_ALL(defend_array, r'(G)')) as opp_score_calculated,
      greatest(coalesce(array_length(split(kicked_array,',')),0),coalesce(array_length(split(defend_array,',')),0)) as max_user_round,
      least(coalesce(array_length(split(kicked_array,',')),0),coalesce(array_length(split(defend_array,',')),0)) as min_user_round,
      forfeit_flag,
      minimize_flag,
      starting_role


    from 
      `football-world-4c5b7.game_play.game_details`
    ) as a 

  left join 

    (select distinct 
      event_date,
      country,
      app_version,
      user_pseudo_id,
      room_name,
      opponent_type,
      user_won,
      user_score,
      opp_score,
      regexp_replace(kicked_array, r'[, "\]\[]', '') as kicked_array,
      regexp_replace(defend_array, r'[, "\]\[]', '') as defend_array,
      ARRAY_LENGTH(REGEXP_EXTRACT_ALL(kicked_array, r'(G)')) as user_score_calculated,
      ARRAY_LENGTH(REGEXP_EXTRACT_ALL(defend_array, r'(G)')) as opp_score_calculated,
      greatest(coalesce(array_length(split(kicked_array,',')),0),coalesce(array_length(split(defend_array,',')),0)) as max_opp_round,
      least(coalesce(array_length(split(kicked_array,',')),0),coalesce(array_length(split(defend_array,',')),0)) as min_opp_round

    from 
      `football-world-4c5b7.game_play.game_details`

    where 
      opponent_type in ("Real")
    ) as b 

  on 
    a.room_name = b.room_name
    and a.user_pseudo_id != b.user_pseudo_id

where 
  a.opponent_type in ('Bot')
  or (a.opponent_type in ('Real') and b.user_pseudo_id is not null)
  
) as a 

order by 
  1,2,3