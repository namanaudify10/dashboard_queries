insert into `football-world-4c5b7.game_play.game_details`

with 
  base_table as 

  (select 
    event_date,
    user_pseudo_id,
    event_timestamp,
    extract(date from timestamp_micros(event_timestamp) AT TIME ZONE "Asia/Kolkata") as date,
    extract(hour from timestamp_micros(event_timestamp) AT TIME ZONE "Asia/Kolkata") as hour,
    event_name,
    app_info.version,
    geo.country,
    geo.city,
    device.mobile_brand_name,
    device.operating_system_version as os_version,

    a.key as a_key,
    a.value.string_value as a_string_value,
    a.value.int_value as a_int_value, 
    a.value.double_value as a_double_value,

    b.key as b_key,
    b.value.string_value as b_string_value,
    b.value.int_value as b_int_value,
    b.value.double_value as b_double_value


  from 
    `football-world-4c5b7.analytics_341800730.events_intraday_*`, unnest(event_params) as a, unnest(event_params) as b 
  where  
    -- _TABLE_SUFFIX between '20230115' and '20230208'
    -- PARSE_DATE('%Y%m%d',_TABLE_SUFFIX) between date_add(current_date(), interval -1 day) and date_add(current_date(), interval -1 day)

    PARSE_DATE('%Y%m%d',_TABLE_SUFFIX) = extract(date from timestamp_sub(current_timestamp() ,INTERVAL 1 HOUR) at TIME ZONE "Asia/Kolkata")
    
    and timestamp_micros(event_timestamp) between timestamp_sub(current_timestamp() ,INTERVAL 70 MINUTE) and current_timestamp()
    
    and user_pseudo_id is not null 
    and event_name in ('user_match_found_event', 'game_play_started_event', 'game_ended_event', 'kicked_event', 'defended_event','game_forfeit_event','user_engagement', 'striker_swipe_event')
    -- and geo.country not in ('India')
  group by  
    1,2,3,4,5,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
  ),


  kicked_table as  

  (select
    a.event_timestamp,
    a.user_pseudo_id,
    a.room_name,
    a.round,
    b.status,
    case when status = 'TimeOut' then null else speed end as speed,
    case when status = 'TimeOut' then null else x_coord end as x_coord,
    case when status = 'TimeOut' then null else y_coord end as y_coord

  from 

    (select
      event_timestamp,
      user_pseudo_id,
      a_string_value as room_name,
      b_int_value as round 

    from 
      base_table
    where 
      event_name in ('kicked_event')
      and a_key in ('room_name')
      and b_key in ('player_round_count')

    group by 
      1,2,3,4
    ) as a 

  join 

    (select distinct 
      event_timestamp,
      user_pseudo_id,
      substr(a_string_value, 1, 1) as status,
      round(b_double_value, 0) as speed

    from 
      base_table
    where 
      event_name in ('kicked_event')
      and a_key in ('status')
      and b_key in ('speed')
    ) as b 
  on 
    a.user_pseudo_id = b.user_pseudo_id
    and a.event_timestamp = b.event_timestamp

  join 

    (select distinct 
      event_timestamp,
      user_pseudo_id,
      round(a_double_value, 2) as x_coord,
      round(b_double_value, 2) as y_coord

    from 
      base_table
    where 
      event_name in ('kicked_event')
      and a_key in ('BallFinalPositionXCoordinate')
      and b_key in ('BallFinalPositionYCoordinate')
    ) as c 
  on 
    a.user_pseudo_id = c.user_pseudo_id
    and a.event_timestamp = c.event_timestamp

  ),


  defend_table as 

  (select
    a.event_timestamp,
    a.user_pseudo_id,
    a.room_name,
    a.round,
    b.status

  from 

    (select
      event_timestamp,
      user_pseudo_id,
      a_string_value as room_name,
      b_int_value as round 

    from 
      base_table
    where 
      event_name in ('defended_event')
      and a_key in ('room_name')
      and b_key in ('opponent_round_count')
    group by 
      1,2,3,4
    ) as a 

  join 

    (select
      event_timestamp,
      user_pseudo_id,
      substr(a_string_value, 1, 1) as status 

    from 
      base_table
    where 
      event_name in ('defended_event')
      and a_key in ('status')
    group by 
      1,2,3
    ) as b 
  on 
    a.user_pseudo_id = b.user_pseudo_id
    and a.event_timestamp = b.event_timestamp
  ),


  striker_swipe_table as  

  (select
    a.user_pseudo_id,
    a.room_name,

    FORMAT("%T", ARRAY_AGG(draw_len order by a.event_timestamp)) as draw_len,
    FORMAT("%T", ARRAY_AGG(draw_time order by a.event_timestamp)) as draw_time,
    FORMAT("%T", ARRAY_AGG(draw_angle order by a.event_timestamp)) as draw_angle,
    FORMAT("%T", ARRAY_AGG(striker_delay order by a.event_timestamp)) as striker_delay,

    FORMAT("%T", ARRAY_AGG(start_y order by a.event_timestamp)) as start_x,
    FORMAT("%T", ARRAY_AGG(start_y order by a.event_timestamp)) as start_y,

    FORMAT("%T", ARRAY_AGG(median_x order by a.event_timestamp)) as median_x,
    FORMAT("%T", ARRAY_AGG(median_y order by a.event_timestamp)) as median_y,
    FORMAT("%T", ARRAY_AGG(end_x order by a.event_timestamp)) as end_x,
    FORMAT("%T", ARRAY_AGG(end_y order by a.event_timestamp)) as end_y,

  from 

    (select distinct 
      event_timestamp,
      user_pseudo_id,
      a_string_value as room_name

    from 
      base_table
    where 
      event_name in ('striker_swipe_event')
      and a_key in ('room_name')
    ) as a 

  join 

    (select distinct
      event_timestamp,
      user_pseudo_id,
      round(a_double_value, 2) as draw_len,
      round(b_double_value, 2) as draw_time

    from 
      base_table
    where 
      event_name in ('striker_swipe_event')
      and a_key in ('draw_length')
      and b_key in ('draw_time_taken')
    ) as b 
  on 
    a.user_pseudo_id = b.user_pseudo_id
    and a.event_timestamp = b.event_timestamp

  join 

    (select distinct
      event_timestamp,
      user_pseudo_id,
      round(a_double_value, 2) as start_x,
      round(b_double_value, 2) as start_y

    from 
      base_table
    where 
      event_name in ('striker_swipe_event')
      and a_key in ('start_x')
      and b_key in ('start_y')
    ) as c 
  on 
    a.user_pseudo_id = c.user_pseudo_id
    and a.event_timestamp = c.event_timestamp

  join 

    (select distinct
      event_timestamp,
      user_pseudo_id,
      round(a_double_value, 2) as end_x,
      round(b_double_value, 2) as end_y

    from 
      base_table
    where 
      event_name in ('striker_swipe_event')
      and a_key in ('end_x')
      and b_key in ('end_y')
    ) as d 
  on 
    a.user_pseudo_id = d.user_pseudo_id
    and a.event_timestamp = d.event_timestamp

  join 

    (select distinct
      event_timestamp,
      user_pseudo_id,
      round(a_double_value, 2) as median_x,
      round(b_double_value, 2) as median_y

    from 
      base_table
    where 
      event_name in ('striker_swipe_event')
      and a_key in ('median_x')
      and b_key in ('median_y')
    ) as e 
  on 
    a.user_pseudo_id = e.user_pseudo_id
    and a.event_timestamp = e.event_timestamp

  join 

    (select distinct
      event_timestamp,
      user_pseudo_id,
      round(a_double_value, 2) as draw_angle,
      round(b_double_value, 2) as striker_delay

    from 
      base_table
    where 
      event_name in ('striker_swipe_event')
      and a_key in ('draw_angle')
      and b_key in ('striker_delay')
    ) as f 
  on 
    a.user_pseudo_id = f.user_pseudo_id
    and a.event_timestamp = f.event_timestamp

  group by 
    1,2
  )


-- main query starts here 

select
  a.event_date,
  country,
  city,
  mobile_brand_name,
  os_version,
  app_version,
  a.user_pseudo_id,
  a.event_timestamp,
  ga_session_id,
  a.room_name,
  opponent_type,
  starting_role,
  user_won,
  user_score,
  opp_score,
  kicked_array,
  kicked_speed_array,
  kicked_X_array,
  kicked_Y_array,
  defend_array,

  draw_len,
  draw_time,
  draw_angle,
  striker_delay,
  start_x,
  start_y,
  median_x,
  median_y,
  end_x,
  end_y,

  a.date,
  a.hour,
  case
    when i.room_name is not null then 1
    else 0
  end as forfeit_flag,
  j.minimize_flag


from 

  (select
    event_date,
    date,
    hour,
    event_timestamp,
    user_pseudo_id,
    country,
    city,
    mobile_brand_name,
    os_version,
    version as app_version,
    a_string_value as room_name,
    b_int_value as ga_session_id


  from 
    base_table 
  where 
    event_name in ('game_play_started_event')
    and a_key in ('room_name')
    and b_key in ('ga_session_id')
    and hour = extract(hour from timestamp_sub(current_timestamp() ,INTERVAL 1 HOUR) at TIME ZONE "Asia/Kolkata")
  group by 
    1,2,3,4,5,6,7,8,9,10,11,12
  ) as a  

join 

  (select
    user_pseudo_id,
    a_string_value as room_name,
    b_string_value as opponent_type
  from 
    base_table
  where
    event_name in ('user_match_found_event')
    and a_key in('room_name')
    and b_key in ('opponent_type')
  group by 
    1,2,3
  ) as b  

on
  a.user_pseudo_id = b.user_pseudo_id
  and a.room_name = b.room_name

left join 

  (select 
    room_name,
    user_pseudo_id,
    FORMAT("%T", ARRAY_AGG(status order by round)) as kicked_array,
    FORMAT("%T", ARRAY_AGG(speed order by round)) as kicked_speed_array,
    FORMAT("%T", ARRAY_AGG(x_coord order by round)) as kicked_X_array,
    FORMAT("%T", ARRAY_AGG(y_coord order by round)) as kicked_Y_array

  from 
    kicked_table
  group by 
    1,2
  ) as c 
on 

  a.user_pseudo_id = c.user_pseudo_id
  and a.room_name = c.room_name

left join 

  (select
    room_name,
    user_pseudo_id,
    FORMAT("%T", ARRAY_AGG(status order by round)) as defend_array
  from 
    defend_table
  group by 
    1,2
  ) as d 
on 

  a.user_pseudo_id = d.user_pseudo_id
  and a.room_name = d.room_name

left join 

  (select
    user_pseudo_id,
    a_string_value as room_name,
    b_int_value as user_score
  from 
    base_table
  where
    event_name in ('game_ended_event')
    and a_key in ('room_name')
    and b_key in ('user_score')
  group by 
    1,2,3
  ) as e 

on
  a.user_pseudo_id = e.user_pseudo_id
  and a.room_name = e.room_name

left join 

  (select
    user_pseudo_id,
    a_string_value as room_name,
    b_int_value as opp_score
  from 
    base_table
  where
    event_name in ('game_ended_event')
    and a_key in ('room_name')
    and b_key in ('opp_score')
  group by 
    1,2,3
  ) as f  

on
  a.user_pseudo_id = f.user_pseudo_id
  and a.room_name = f.room_name

left join 

  (select
    user_pseudo_id,
    a_string_value as room_name,
    b_int_value as user_won
  from 
    base_table
  where
    event_name in ('game_ended_event')
    and a_key in ('room_name')
    and b_key in ('user_won')
  group by 
    1,2,3
  ) as g 

on
  a.user_pseudo_id = g.user_pseudo_id
  and a.room_name = g.room_name

left join 
  
  (select
    room_name,
    user_pseudo_id,
    role as starting_role

  from 
    (select
      room_name,
      user_pseudo_id,
      event_timestamp,
      role,
      row_number() over(partition by user_pseudo_id, room_name order by event_timestamp) as rw 

    from 

      (select
        room_name,
        user_pseudo_id,
        event_timestamp,
        'striker' as role 
      from 
        kicked_table
      where 
        round = 1 

      union all

      select
        room_name,
        user_pseudo_id,
        event_timestamp,
        'keeper' as role
      from 
        defend_table
      where 
        round = 1 
      ) as a 
    ) as a 
  where 
    rw = 1 
  ) as h 

on 
  a.user_pseudo_id = h.user_pseudo_id
  and a.room_name = h.room_name

left join
  
  (select
    a_string_value as room_name
  from 
    base_table
  where
    event_name in ('game_forfeit_event')
    and a_key in ('room_name')
  group by 
    1
  ) as i

on
  a.room_name = i.room_name

LEFT JOIN

(SELECT
  j1.room_name,
  max(case
    when j2.user_pseudo_id is not null then 1
    else 0
  end) as minimize_flag
FROM
  (SELECT
    j11.user_pseudo_id,
    j11.room_name,
    j11.event_timestamp as end_timestamp,
    j12.event_timestamp as start_timestamp
  FROM
    (SELECT
      user_pseudo_id,
      event_timestamp,
      a_string_value as room_name
    FROM
      base_table
    WHERE
      event_name in ('game_ended_event')
      and a_key in ('room_name')
    GROUP BY
      1,2,3
    ) as j11

    JOIN
    
    (SELECT
      user_pseudo_id,
      event_timestamp,
      a_string_value as room_name
    FROM
      base_table
    WHERE
      event_name in ('game_play_started_event')
      and a_key in ('room_name')
    GROUP BY
      1,2,3
    ) as j12

    on
      j11.user_pseudo_id = j12.user_pseudo_id
      and j11.room_name = j12.room_name

  GROUP BY 1,2,3,4
  ) as j1


LEFT JOIN
  
  (SELECT
    user_pseudo_id,
    event_timestamp
  FROM
    base_table
  WHERE
    event_name in ('user_engagement')
  ) as j2

on
  j1.user_pseudo_id = j2.user_pseudo_id
  and j2.event_timestamp >= j1.start_timestamp
  and j2.event_timestamp <= j1.end_timestamp
GROUP BY
  1

) as j

on
  a.room_name = j.room_name

left join 

  striker_swipe_table as k 

on 
  a.user_pseudo_id = k.user_pseudo_id
  and a.room_name = k.room_name

order by
  1,7,8