Select 
  a.event_date,
  a.version,
  case
    when language is null then 'g. null'
    when language in ('en') then 'a. English'
    when language in ('ar') then 'b. Arabic'
    when language in ('es') then 'c. Spanish'
    when language in ('fr') then 'd. French'
    when language in ('pt') then 'e. Portugese'
    else 'f. others'
  end as language,
  a.tutorial_state,
  case 
    when country in ('Argentina','Iraq','Brazil','Turkey','Egypt','Bangladesh','India','Algeria','Morocco','Colombia','Pakistan','Mexico','United States','South Africa','Germany', 'United Kingdom') then country
    else 'others' 
  end as country,

  case 
    when continent in ('Africa') then 'a. Africa'
    when continent in ('Americas') then 'b. Americas'
    when continent in ('Asia') then 'c. Asia'
    when continent in ('Europe') then 'd. Europe'
    else 'e. others'
  end as continent,

  case 
    when source is null then 'null'
    when source in ('(direct)', 'google-play', 'google') then source 
    else 'others'
  end as source,

  case 
    when platform_age is null then 'h. null'
    when platform_age = 0 then 'a. d0'
    when platform_age < 4 then 'b. d1-d3'
    when platform_age < 8 then 'c. d4-d7'
    when platform_age < 29 then 'd. w1-w3'
    when platform_age < 181 then 'e. m1-m5'
    when platform_age < 365 then 'f. m6-m11'
    else 'g. y1+'
  end as platform_age_bucket,

  case 
    when PFO_count is null then 'e. null'
    when PFO_count = 0 then 'a. 0'
    when PFO_count < 4 then 'b. 1-3'
    when PFO_count < 11 then 'c. 4-10'
    else 'd. 10+'
  end as PFO_bucket,

  case 
    when os_version is null then 'f. null'
    when os_version in  ('Android 9') then 'a. Android 9'
    when os_version in  ('Android 10') then 'b. Android 10'
    when os_version in  ('Android 11') then 'c. Android 11'
    when os_version in  ('Android 12') then 'd. Android 12'
    else 'e. others'
  end as os_version_bucket,



  case 
    when mobile_brand_name is null then 'h. null'
    when mobile_brand_name in ('Samsung') then 'a. Samsung'
    when mobile_brand_name in ('Xiaomi') then 'b. Xiaomi'
    when mobile_brand_name in ('OPPO') then 'c. OPPO'
    when mobile_brand_name in ('Huawei') then 'd. Huawei'
    when mobile_brand_name in ('Motorola') then 'e. Motorola'
    when mobile_brand_name in ('Realme') then 'f. Realme'
    else 'g. others'
  end as mobile_brand_name_bucket,

  

  case  
    when session_count = 0 then 'a. 0'
    when session_count = 1 then 'b. 1'
    when session_count = 2 then 'c. 2'
    when session_count = 3 then 'd. 3'
    when session_count < 7 then 'e. 4-6'
    when session_count < 11 then 'f. 7-10'
    else 'g. 10+'
  end as session_count_bucket,

  case 
    when game_started_freq is null then 'j. null' 
    when game_started_freq = 0 then 'a. 0'
    when game_started_freq = 1 then 'b. 1'
    when game_started_freq = 2 then 'c. 2'
    when game_started_freq = 3 then 'd. 3'
    when game_started_freq < 8 then 'e. 4-7'
    when game_started_freq < 11 then 'f. 8-10'
    when game_started_freq < 21 then 'g. 11-20'
    when game_started_freq < 51 then 'h. 21-50'
    else 'i. 50+'
  end as game_started_bucket,

  case
    when date_diff(a.event_date,start_date,day) < 7 then 'a. Week 1'
    when date_diff(a.event_date,start_date,day) < 14 then 'b. Week 2'
    when date_diff(a.event_date,start_date,day) < 21 then 'c. Week 3'
    when date_diff(a.event_date,start_date,day) < 28 then 'd. Week 4'
    else 'e. > week 4'
  end as week,

  case 
    when onlines = 0 and offlines > 0 then 'offline'
    when onlines > 0 and offlines = 0 then 'online'
    else 'on_offline'
  end as online_offline_flag,


  case when home_loaded_sessions > 0 then 'home_page_loaded' else 'home_page_not_loaded' end as home_page_load_flag,
  case when app_remove_count > 0 then 'app_removed' else 'not_removed' end as app_remove_flag,

  count(distinct a.user_pseudo_id) as users,
  count(distinct case when session_count > 0 then a.user_pseudo_id end) as DAU,

  sum(session_count) as sessions,
  sum(home_loaded_sessions) as home_loaded_sessions,
  sum(play_button_session) as play_button_clicked,
  sum(game_tier_session) as game_tier_selected,
  sum(matching_started_session) as matching_started,
  sum(matching_found_session) as matching_found,
  sum(game_started_session) as game_started,
  sum(game_ended_session) as game_ended,

  count(distinct case when game_started_freq >= 1 then a.user_pseudo_id end) as game_1_flag,
  count(distinct case when game_started_freq >= 2 then a.user_pseudo_id end) as game_2_flag,
  count(distinct case when game_started_freq >= 3 then a.user_pseudo_id end) as game_3_flag,
  count(distinct case when game_started_freq >= 4 then a.user_pseudo_id end) as game_4_flag,
  count(distinct case when game_started_freq >= 5 then a.user_pseudo_id end) as game_5_flag,
  count(distinct case when game_started_freq >= 6 then a.user_pseudo_id end) as game_more_6_flag,
  count(distinct case when game_started_freq >10 then a.user_pseudo_id end) as game_more_10_flag,

  count(distinct case when game_ended_freq >= 1 then a.user_pseudo_id end) as game_end_1_flag,
  count(distinct case when game_ended_freq >= 2 then a.user_pseudo_id end) as game_end_2_flag,
  count(distinct case when game_ended_freq >= 3 then a.user_pseudo_id end) as game_end_3_flag,
  count(distinct case when game_ended_freq >= 4 then a.user_pseudo_id end) as game_end_4_flag,
  count(distinct case when game_ended_freq >= 5 then a.user_pseudo_id end) as game_end_5_flag,
  count(distinct case when game_ended_freq >= 6 then a.user_pseudo_id end) as game_end_more_6_flag,
  count(distinct case when game_ended_freq >10 then a.user_pseudo_id end) as game_end_more_10_flag,


  sum(play_button_freq) as play_button_clicked_freq,
  sum(game_tier_freq) as game_tier_selected_freq,
  sum(matching_started_freq) as matching_started_freq,
  sum(matching_found_freq) as matching_found_freq,
  sum(game_started_freq) as game_started_freq,
  sum(game_ended_freq) as game_ended_freq,

  -- sum(round((ifnull(eng_time_sec_sv, 0) + ifnull(eng_time_sec_ue, 0)) / 60, 0)) as eng_time_minutes,

  avg(case when avg_loading_time is not null then avg_loading_time end) as avg_loading_time,
  avg(case when avg_matching_time is not null then avg_matching_time end) as avg_matching_time,

  sum(ifnull(bot_matching_found,0)) as bot_matching_found,
  sum(ifnull(real_matching_found,0)) as real_matching_found,
  sum(ifnull(bot_match_won,0)) as bot_match_won,
  sum(ifnull(bot_match_lost,0)) as bot_match_lost,  
  sum(ifnull(bot_match_forfeited,0)) as bot_match_forfeited,
  sum(ifnull(real_match_won,0)) as real_match_won,
  sum(ifnull(real_match_lost,0)) as real_match_lost,
  sum(ifnull(real_match_forfeited,0)) as real_match_forfeited,

  count(distinct case when bag_unlock_started_freq>0 then a.user_pseudo_id end) as bag_unlock_started_users,
  sum(ifnull(bag_unlock_started_freq,0)) as bag_unlock_started_freq,
  count(distinct case when bag_unlocked_freq>0 then a.user_pseudo_id end) as bag_unlocked_users,
  sum(ifnull(bag_unlocked_freq,0)) as bag_unlocked_freq,
  count(distinct case when bag_unzipped_freq>0 then a.user_pseudo_id end) as bag_unzipped_users,
  sum(ifnull(bag_unzipped_freq,0)) as bag_unzipped_freq,

  count(distinct case when skills_visit>0 then a.user_pseudo_id end) as skills_visit_users,
  count(distinct case when (styles_visit>0 or styles_change_freq>0)  then a.user_pseudo_id end) as styles_visit_users,
  count(distinct case when styles_change_freq>0 then a.user_pseudo_id end) as styles_change_users,

  AVG(case when real_matching_found > 0 then real_match_won / real_matching_found end) as real_win_ratio,
  AVG(case when bot_matching_found > 0 then bot_match_won / bot_matching_found end) as bot_win_ratio,
  AVG(case when real_matching_found is not null then (bot_match_won + real_match_won) /(real_matching_found + bot_matching_found) end) as overall_win_ratio,

  sum(app_remove_count) as app_removes,

  sum(d1) as d1_retention,
  sum(d3) as d3_retention,
  sum(d7) as d7_retention,
  sum(case when N7_count >= 4 then 1 else 0 end) as N7_4

from 
  `football-world-4c5b7.Naman_table.daily_engagement_base` as a 

left join 

  (select
    a.event_date,
    a.user_pseudo_id,
    count(distinct case when date_diff(b.event_date, a.event_date, day) = 1 then b.user_pseudo_id end) as d1,
    count(distinct case when date_diff(b.event_date, a.event_date, day) = 3 then b.user_pseudo_id end) as d3,
    count(distinct case when date_diff(b.event_date, a.event_date, day) = 7 then b.user_pseudo_id end) as d7,
    count(distinct b.event_date) as N7_count

  from  
    
    (select
      event_date,
      user_pseudo_id
    from 
      `football-world-4c5b7.Naman_table.daily_engagement_base`
    where 
      event_date between date_add(current_date(), interval -50 day) and date_add(current_date(), interval -1 day)
      and session_count > 0
    group by 
      1,2
    ) as a 

  left join 

    (select
      event_date,
      user_pseudo_id
    from 
      `football-world-4c5b7.Naman_table.daily_engagement_base`
    where 
      event_date between date_add(current_date(), interval -50 day) and date_add(current_date(), interval -1 day)
      and session_count > 0
    group by 
      1,2
    ) as b 

  on 
    date_diff(b.event_date, a.event_date, day) between 1 and 7  
    and a.user_pseudo_id = b.user_pseudo_id

  group by 
    1,2 
  ) as b 

on 
  a.event_date = b.event_date 
  and a.user_pseudo_id = b.user_pseudo_id

left join
  
  (select
    version,
    min(event_date) as start_date
   from
    `football-world-4c5b7.Naman_table.daily_engagement_base`
   group by
    1
  ) as c

  on a.version = c.version

where  
  a.event_date between date_add(current_date(), interval -50 day) and date_add(current_date(), interval -1 day)

group by 
  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
order by 
  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17