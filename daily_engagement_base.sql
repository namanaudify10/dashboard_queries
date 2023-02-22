with

  base_table as

  (SELECT
      PARSE_DATE('%Y%m%d',event_date) as event_date,
      TIMESTAMP_MICROS(event_timestamp) as event_time,
      TIMESTAMP_MICROS(user_first_touch_timestamp) as first_open_time,
      user_pseudo_id,
      app_info.version,
      geo.continent,
      geo.sub_continent,
      geo.country,
      geo.city,
      device.category,
      device.mobile_brand_name,
      device.operating_system_version,
      REGEXP_SUBSTR(device.language, '[^-]+') as language,
      traffic_source.medium,
      traffic_source.source,
      event_name,
      a1.key ,
      a1.value.string_value,
      a1.value.int_value,
      a1.value.float_value,
      a1.value.double_value,
      a2.key as key_a2,
      a2.value.string_value as string_value_a2,
      a2.value.int_value as int_value_a2,
      a2.value.float_value as float_value_a2,
      a2.value.double_value as double_value_a2

    FROM
      -- `football-world-4c5b7.analytics_341800730.events_*`, unnest(event_params) as a1, unnest(event_params) as a2
      `football-world-4c5b7.analytics_341800730.events_intraday_*`, unnest(event_params) as a1, unnest(event_params) as a2

    WHERE
      -- _TABLE_SUFFIX = '20230112'
      PARSE_DATE('%Y%m%d',_TABLE_SUFFIX) between date_add(current_date(), interval -1 day) and date_add(current_date(), interval -1 day)
      AND event_name in (
        'game_ended_event',   
        'game_play_started_event',
        'play_button_clicked_event',
        'user_engagement',
        'screen_view',
        'first_open',
        'app_remove',
        'session_start',
        'game_tier_selected_event',
        'user_matching_started_event',
        'user_match_found_event',
        'loading_time_event',
        'network_status_event',
        'bag_unlock_started_event',
        'bag_unlocked_event',
        'bag_cards_detail',
        'tutorial_event',
        'landing_page_event',
        'style_change_event'
      ) 
      and user_pseudo_id is not null 
      -- and geo.city not in ('Bengaluru')

  GROUP BY 
    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
  )


SELECT 
  b.*,
  c.* except (event_date, user_pseudo_id),
  d.* except (event_date, user_pseudo_id),
  e.total_matchmaking_time / matching_found_freq as avg_matching_time
FROM  
  
  (SELECT
    * except (row_num)
  FROM
    (SELECT
      event_date,
      user_pseudo_id,
      version,
      operating_system_version as os_version,
      continent,
      sub_continent,
      country,
      medium,
      source,
      category as device_category,
      mobile_brand_name,
      language,
      row_number() over (partition by user_pseudo_id,event_date order by rand()) as row_num

    FROM
      base_table
    WHERE
      date_diff(date(event_time), date(first_open_time), day) >= 0
    GROUP BY
      1,2,3,4,5,6,7,8,9,10,11,12
    )
  WHERE
    row_num = 1
  ) as b


LEFT JOIN

  (SELECT
    event_date,
    user_pseudo_id,

    min(date_diff(date(event_time), date(first_open_time), day)) as platform_age,

    max(case when event_name in ('first_open') and key in ('previous_first_open_count') then int_value end) as PFO_count,
    
    count(distinct case when event_name in ('session_start') then event_time end) as session_count,

    count(distinct case when event_name in ('screen_view') and key in ('firebase_screen') and string_value in ('Home') and key_a2 in ('ga_session_id') then int_value_a2 end) as home_loaded_sessions,

    count(distinct case when event_name in ('network_status_event') and  key in ('status') and string_value in ('online') then event_time end) as onlines,
    count(distinct case when event_name in ('network_status_event') and  key in ('status') and string_value in ('offline') then event_time end) as offlines,

    count(distinct case when event_name in ('play_button_clicked_event') and key in ('ga_session_id') then int_value end) as play_button_session,
    count(distinct case when event_name in ('play_button_clicked_event') then event_time end) as play_button_freq,

    count(distinct case when event_name in ('game_tier_selected_event') and key in ('ga_session_id') then int_value end) as game_tier_session,
    count(distinct case when event_name in ('game_tier_selected_event') then event_time end) as game_tier_freq,

    count(distinct case when event_name in ('user_matching_started_event') and key in ('ga_session_id') then int_value end) as matching_started_session,
    count(distinct case when event_name in ('user_matching_started_event') then event_time end) as matching_started_freq,

    count(distinct case when event_name in ('user_match_found_event') and key in ('ga_session_id') then int_value end) as matching_found_session,  
    count(distinct case when event_name in ('user_match_found_event') then event_time end) as matching_found_freq,

    count(distinct case when event_name in ('game_play_started_event') and key in ('ga_session_id') then int_value end) as game_started_session,  
    count(distinct case when event_name in ('game_play_started_event') then event_time end) as game_started_freq,

    count(distinct case when event_name in ('game_ended_event') and key in ('ga_session_id') then int_value end) as game_ended_session,
    count(distinct case when event_name in ('game_ended_event') then event_time end) as game_ended_freq,

    count(distinct case when event_name in ('bag_unlock_started_event') then event_time end) as bag_unlock_started_freq,
    count(distinct case when event_name in ('bag_unlocked_event') then event_time end) as bag_unlocked_freq,
    count(distinct case when event_name in ('bag_cards_detail') then event_time end) as bag_unzipped_freq,

    max(case when event_name in ('tutorial_event') and key in ('tutorial_state') then int_value end) as tutorial_state,

    count(distinct case when event_name in ('landing_page_event') and key in ('option_clicked') and string_value in ('Skills') then event_time end) as skills_visit,
    count(distinct case when event_name in ('landing_page_event') and key in ('option_clicked') and string_value in ('Styles') then event_time end) as styles_visit,
    count(distinct case when event_name in ('style_change_event') and key in ('new_id') and key_a2 in ('previous_id') and (string_value != string_value_a2) then event_time end) as styles_change_freq,

    sum(case when event_name in ('screen_view') and key in ('engagement_time_msec') then int_value / 1000 end) eng_time_sec_sv,
    sum(case when event_name in ('user_engagement') and key in ('engagement_time_msec') then int_value / 1000 end) eng_time_sec_ue,



    avg(case when event_name in ('loading_time_event') and key in ('time_spent') then cast(replace(replace(string_value,',','.'),'/','.') as float64) end) as avg_loading_time,
    count(distinct case when event_name in ('app_remove') then event_time end) as app_remove_count

  FROM
    base_table

  GROUP BY
    1,2
  ) as c
on b.user_pseudo_id = c.user_pseudo_id
and b.event_date = c.event_date

LEFT JOIN

  (SELECT
    d1.event_date,
    d1.user_pseudo_id,

    count(distinct case when d1.opponent_type in ('Bot') then d1.room_name end) as bot_matching_found,
    count(distinct case when d1.opponent_type in ('Real') then d1.room_name end) as real_matching_found,

    count(distinct case when d1.opponent_type in ('Bot') and d2.result = 1 then d1.room_name end) as bot_match_won,
    count(distinct case when d1.opponent_type in ('Bot') and d2.result = 0 then d1.room_name end) as bot_match_lost,
    count(distinct case when d1.opponent_type in ('Bot') and d2.result is null then d1.room_name end) as bot_match_forfeited,

    count(distinct case when d1.opponent_type in ('Real') and d2.result = 1 then d1.room_name end) as real_match_won,
    count(distinct case when d1.opponent_type in ('Real') and d2.result = 0 then d1.room_name end) as real_match_lost,
    count(distinct case when d1.opponent_type in ('Real') and d2.result is null then d1.room_name end) as real_match_forfeited

  FROM
    (SELECT
      event_date,
      user_pseudo_id,
      string_value as room_name,
      case
        when string_value_a2 in ('31341') then 'Bot'
        else 'Real'
      end as opponent_type

    FROM 
      base_table

    WHERE
      event_name in ('game_play_started_event')
      and key in ('room_name')
      and key_a2 in ('opp_id')

    GROUP BY
      1,2,3,4
    ) as d1

  LEFT JOIN

    (SELECT
      event_date,
      user_pseudo_id,
      string_value as room_name,
      int_value_a2 as result

     FROM
       base_table

     WHERE
      event_name in ('game_ended_event')
      and key in ('room_name')
      and key_a2 in ('user_won')

     GROUP BY
      1,2,3,4
     ) as d2

    on d1.event_date = d2.event_date 
    and d1.user_pseudo_id = d2.user_pseudo_id
    and d1.room_name = d2.room_name

  GROUP BY 
    1,2
  ) as d

on b.user_pseudo_id = d.user_pseudo_id
and b.event_date = d.event_date

LEFT JOIN

  (SELECT
    event_date,
    user_pseudo_id,
    sum(timestamp_diff(event_time,previous_event_time,second)) as total_matchmaking_time
   FROM
    (SELECT
      event_date,
      user_pseudo_id,
      int_value,
      event_time,
      event_name,
      lag(event_name) over (partition by user_pseudo_id order by event_time) as previous_event,
      lag(event_time) over (partition by user_pseudo_id order by event_time) as previous_event_time

     FROM
      base_table

     WHERE
      event_name in ('user_match_found_event','user_matching_started_event')
      and key in ('ga_session_id')

     GROUP BY 1,2,3,4,5
    )
  WHERE
    event_name in ('user_match_found_event')
    and previous_event in ('user_matching_started_event')

  GROUP BY
    1,2
  ) as e

on b.user_pseudo_id = e.user_pseudo_id
and b.event_date = e.event_date