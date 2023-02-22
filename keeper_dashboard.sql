----keeper dashboard-----

with

  base_table as

  (SELECT
      PARSE_DATE('%Y%m%d',event_date) as event_date,
      event_timestamp,
      user_pseudo_id,
      app_info.version,
      geo.country,
      event_name,
      a1.key ,
      a1.value.string_value,
      a1.value.int_value,
      a1.value.double_value,
      a2.key as key_a2,
      a2.value.string_value as string_value_a2,
      a2.value.int_value as int_value_a2,
      a2.value.double_value as double_value_a2,

    FROM
      `football-world-4c5b7.analytics_341800730.events_intraday_*`, unnest(event_params) as a1, unnest(event_params) as a2

    WHERE
      -- _TABLE_SUFFIX >= '20230124'
      PARSE_DATE('%Y%m%d',_TABLE_SUFFIX) between date_add(current_date(), interval -1 day) and date_add(current_date(), interval -1 day)
      AND event_name in (
        'first_open',
        'keeper_swipe_event',
        'defended_event',
        'game_play_started_event',
        'opposition_kicked_event'
      ) 
      and user_pseudo_id is not null 
      -- and app_info.version in ('2.02.01','2.01.02','2.02.02','2.02.01.01')
      -- and geo.city not in ('Bengaluru')

  GROUP BY 
    1,2,3,4,5,6,7,8,9,10,11,12,13,14
  )

 SELECT 
  b.*,
  c.* except (event_date, user_pseudo_id),
  d.* except (event_date, user_pseudo_id),
  case
  	when d.speed <= 20 then 'a. <=20'
  	when d.speed <=50 then 'b. 21-50'
  	when d.speed <=80 then 'c. 51-80'
  	when d.speed <=110 then 'd. 81-110'
  	when d.speed <=149 then 'e. 111-149'
  	when d.speed >=150 then 'f. >=150'
  end as speed_bucket,
  case
  	when d.calculated_speed <= 20 then 'a. <=20'
  	when d.calculated_speed <=50 then 'b. 21-50'
  	when d.calculated_speed <=80 then 'c. 51-80'
  	when d.calculated_speed <=110 then 'd. 81-110'
  	when d.calculated_speed <=149 then 'e. 111-149'
  	when d.calculated_speed >=150 then 'f. >=150'
  end as speed_calculated_bucket,
  e.* except (event_date, user_pseudo_id,event_timestamp,round,room_name,response_time),
  case
  	when e.swipe_angle >=0 and swipe_angle <=60 then 'a. 0-60'
  	when e.swipe_angle > 60 and swipe_angle <= 120 then 'b. 60-120'
  	when e.swipe_angle > 120 and swipe_angle <= 180 then 'c. 120-180'
  	when e.swipe_angle >= -180 and swipe_angle <= -120 then 'd. 180-240'
  	when e.swipe_angle > -120 and swipe_angle <= -60 then 'e. 240-300'
  	when e.swipe_angle > -60 and swipe_angle < 0 then 'f. 300-360'
  end as swipe_angle_bucket,
  case
  	when e.response_time is null then 'm. null'
  	when e.response_time <= 0 then 'a. <=0'
  	when e.response_time <= 0.1 then 'b. 0-0.1'
  	when e.response_time <= 0.2 then 'c. 0.1-0.2'
  	when e.response_time <= 0.3 then 'd. 0.2-0.3'
  	when e.response_time <= 0.4 then 'e. 0.3-0.4'
  	when e.response_time <= 0.5 then 'f. 0.4-0.5'
  	when e.response_time <= 0.6 then 'g. 0.5-0.6'
  	when e.response_time <= 0.7 then 'h. 0.6-0.7'
  	when e.response_time <= 0.8 then 'i. 0.7-0.8'
  	when e.response_time <= 0.9 then 'j. 0.8-0.9'
  	when e.response_time <= 1 then 'k. 0.9-1.0'
  	when e.response_time > 1 then 'l. >1'
  end as response_time,
  case	
  	when e.fraction <= 0.1 then 'a. <=0.1'
  	when e.fraction <= 0.2 then 'b. 0.1-0.2'
  	when e.fraction <= 0.3 then 'c. 0.2-0.3'
  	when e.fraction <= 0.4 then 'd. 0.3-0.4'
  	when e.fraction <= 0.5 then 'e. 0.4-0.5'
  	when e.fraction <= 0.6 then 'f. 0.5-0.6'
  	when e.fraction <= 0.7 then 'g. 0.6-0.7'
  	when e.fraction <= 0.8 then 'h. 0.7-0.8'
  	when e.fraction <= 0.9 then 'i. 0.8-0.9'
  	when e.fraction <= 1.0 then 'j. 0.9-1.0'
  	when e.fraction > 1.0 then 'k. >1'
  end as fraction_bucket,
  f.goalkeeping_attribute,
  case 
    when (g.curve * e.flight_time * e.flight_time / 2 ) <= -1 then 'a. <=-1'
  	when (g.curve * e.flight_time * e.flight_time / 2 ) <= -0.8 then 'b. -1 ~ -0.8'
  	when (g.curve * e.flight_time * e.flight_time / 2 ) <= -0.6 then 'c. -0.8 ~ -0.6'
  	when (g.curve * e.flight_time * e.flight_time / 2 ) <= -0.4 then 'd. -0.6 ~ -0.4'
  	when (g.curve * e.flight_time * e.flight_time / 2 ) <= -0.2 then 'e. -0.4 ~ -0.2'
  	when (g.curve * e.flight_time * e.flight_time / 2 ) < 0 then 'f. -0.2 ~ 0'
  	when (g.curve * e.flight_time * e.flight_time / 2 ) = 0.0 then 'g. 0.0'
  	when (g.curve * e.flight_time * e.flight_time / 2 ) <= 0.2 then 'h. 0 ~ 0.2'
  	when (g.curve * e.flight_time * e.flight_time / 2 ) <= 0.4 then 'i. 0.2 ~ 0.4'
  	when (g.curve * e.flight_time * e.flight_time / 2 ) <= 0.6 then 'j. 0.4 ~ 0.6'
  	when (g.curve * e.flight_time * e.flight_time / 2 ) <= 0.8 then 'k. 0.6 ~ 0.8'
  	when (g.curve * e.flight_time * e.flight_time / 2 ) <= 1 then 'l. 0.8 ~ 1'
  	when (g.curve * e.flight_time * e.flight_time / 2 ) > 1 then 'm. >1'
  	else 'n. null'
  end as curve
FROM  
  
    (SELECT
      event_date,
      user_pseudo_id,
      version,
      country
    FROM
      base_table
    GROUP BY
      1,2,3,4
  	) as b


LEFT JOIN

  (SELECT
    event_date,
    user_pseudo_id,
    max(case when event_name in ('first_open') and key in ('previous_first_open_count') then int_value end) as PFO_count
   FROM
   	base_table
   GROUP BY
   	1,2
   ) as c
  
on
  b.user_pseudo_id = c.user_pseudo_id
  and b.event_date = c.event_date

LEFT JOIN
	
  (select
    d1.event_date,
    d1.event_timestamp,
    d1.user_pseudo_id,
    d1.room_name,
    d1.response_status,
    d2.Xballcord,
    d2.Yballcord,
    case 
	    when d2.Xballcord between -3.086 and -2.4688 and d2.Yballcord between 1.74 and 2.32 then 'aa. L1'
	    when d2.Xballcord between -2.4688 and -1.8516 and d2.Yballcord between 1.74 and 2.32 then 'ab. L2'
	    when d2.Xballcord between -1.8516 and -1.2344 and d2.Yballcord between 1.74 and 2.32 then 'ac. L3'
	    when d2.Xballcord between -1.2344 and -0.6172 and d2.Yballcord between 1.74 and 2.32 then 'ad. L4'
	    when d2.Xballcord between -0.6172 and 0 and d2.Yballcord between 1.74 and 2.32 then 'ae. L5'
	    when d2.Xballcord between 0 and 0.6172 and d2.Yballcord between 1.74 and 2.32 then 'ba. R1'
	    when d2.Xballcord between 0.6172 and 1.2344 and d2.Yballcord between 1.74 and 2.32 then 'bb. R2'
	    when d2.Xballcord between 1.2344 and 1.8516 and d2.Yballcord between 1.74 and 2.32 then 'bc. R3'
	    when d2.Xballcord between 1.8516 and 2.4688 and d2.Yballcord between 1.74 and 2.32 then 'bd. R4'
	    when d2.Xballcord between 2.4688 and 3.086 and d2.Yballcord between 1.74 and 2.32 then 'be. R5'
	 
	    when d2.Xballcord between -3.086 and -2.4688 and d2.Yballcord between 1.16 and 1.74 then 'af. L6'
	    when d2.Xballcord between -2.4688 and -1.8516 and d2.Yballcord between 1.16 and 1.74 then 'ag. L7'
	    when d2.Xballcord between -1.8516 and -1.2344 and d2.Yballcord between 1.16 and 1.74 then 'ah. L8'
	    when d2.Xballcord between -1.2344 and -0.6172 and d2.Yballcord between 1.16 and 1.74 then 'ai. L9'
	    when d2.Xballcord between -0.6172 and 0 and d2.Yballcord between 1.16 and 1.74 then 'aj. L10'
	    when d2.Xballcord between 0 and 0.6172 and d2.Yballcord between 1.16 and 1.74 then 'bf. R6'
	    when d2.Xballcord between 0.6172 and 1.2344 and d2.Yballcord between 1.16 and 1.74 then 'bg. R7'
	    when d2.Xballcord between 1.2344 and 1.8516 and d2.Yballcord between 1.16 and 1.74 then 'bh. R8'
	    when d2.Xballcord between 1.8516 and 2.4688 and d2.Yballcord between 1.16 and 1.74 then 'bi. R9'
	    when d2.Xballcord between 2.4688 and 3.086 and d2.Yballcord between 1.16 and 1.74 then 'bj. R10'
	 
	    when d2.Xballcord between -3.086 and -2.4688 and d2.Yballcord between 0.58 and 1.16  then 'ak. L11'
	    when d2.Xballcord between -2.4688 and -1.8516 and d2.Yballcord between 0.58 and 1.16 then 'al. L12'
	    when d2.Xballcord between -1.8516 and -1.2344 and d2.Yballcord between 0.58 and 1.16 then 'am. L13'
	    when d2.Xballcord between -1.2344 and -0.6172 and d2.Yballcord between 0.58 and 1.16 then 'an. L14'
	    when d2.Xballcord between -0.6172 and 0 and d2.Yballcord between 0.58 and 1.16 then 'ao. L15'
	    when d2.Xballcord between 0 and 0.6172 and d2.Yballcord between 0.58 and 1.16 then 'bk. R11'
	    when d2.Xballcord between 0.6172 and 1.2344 and d2.Yballcord between 0.58 and 1.16 then 'bl. R12'
	    when d2.Xballcord between 1.2344 and 1.8516 and d2.Yballcord between 0.58 and 1.16 then 'bm. R13'
	    when d2.Xballcord between 1.8516 and 2.4688 and d2.Yballcord between 0.58 and 1.16 then 'bn. R14'
	    when d2.Xballcord between 2.4688 and 3.086 and d2.Yballcord between 0.58 and 1.16 then 'bo. R15'
	 
	    when d2.Xballcord between -3.086 and -2.4688 and d2.Yballcord between 0 and 0.58 then 'ap. L16'
	    when d2.Xballcord between -2.4688 and -1.8516 and d2.Yballcord between 0 and 0.58 then 'aq. L17'
	    when d2.Xballcord between -1.8516 and -1.2344 and d2.Yballcord between 0 and 0.58 then 'ar. L18'
	    when d2.Xballcord between -1.2344 and -0.6172 and d2.Yballcord between 0 and 0.58 then 'as. L19'
	    when d2.Xballcord between -0.6172 and 0 and d2.Yballcord between 0 and 0.58 then 'at. L20'
	    when d2.Xballcord between 0 and 0.6172 and d2.Yballcord between 0 and 0.58 then 'bp. R16'
	    when d2.Xballcord between 0.6172 and 1.2344 and d2.Yballcord between 0 and 0.58 then 'bq. R17'
	    when d2.Xballcord between 1.2344 and 1.8516 and d2.Yballcord between 0 and 0.58 then 'br. R18'
	    when d2.Xballcord between 1.8516 and 2.4688 and d2.Yballcord between 0 and 0.58 then 'bs. R19'
	    when d2.Xballcord between 2.4688 and 3.086 and d2.Yballcord between 0 and 0.58 then 'bt. R20'

	    when d2.Xballcord < -3.086 and d2.Yballcord between 0 and 1.16 then 'ca. O1'
	    when d2.Xballcord < -3.086 and d2.Yballcord between 1.16 and 2.32 then 'cb. O2'
	    when d2.Xballcord < -3.086 and d2.Yballcord > 2.32 then 'cc. O3'
	    when d2.Xballcord between -3.086 and 3.086 and d2.Yballcord > 2.32 then 'cd. O4'
	    when d2.Xballcord > 3.086 and d2.Yballcord between 0 and 1.16 then 'cg. O7'
	    when d2.Xballcord > 3.086 and d2.Yballcord between 1.16 and 2.32 then 'cf. O6'
	    when d2.Xballcord > 3.086 and d2.Yballcord > 2.32 then 'ce. O5'
	 
	    else 'd. Other'
    end as boxes,
    (atan2(d2.Yballcord,d2.Xballcord)* 180/acos(-1)) as ball_angle,
    case
    	when (atan2(d2.Yballcord,d2.Xballcord)* 180/acos(-1)) < 90 and ((atan2(d2.Yballcord,d2.Xballcord)* 180/acos(-1)) + 30) > 90 then 100
    	else ((atan2(d2.Yballcord,d2.Xballcord)* 180/acos(-1)) + 30)
    end as max_ball_angle,
    case
    	when (atan2(d2.Yballcord,d2.Xballcord)* 180/acos(-1)) > 90 and ((atan2(d2.Yballcord,d2.Xballcord)* 180/acos(-1)) - 30) < 90 then 80
    	else ((atan2(d2.Yballcord,d2.Xballcord)* 180/acos(-1)) - 30)
    end as min_ball_angle,
    case
    	when d3.swipe_direction = -1 then 'Left'
    	when d3.swipe_direction = 0 then 'Middle'
    	when d3.swipe_direction = 1 then 'Right'
    end as swipe_direction,
    d3.round,
    case
    	when d4.opp_id in ('31341') then 'Bot'
    	else 'Real'
    end as opponent_type,
    d4.status,
    d5.speed as velocity,
    round(32.5*d5.speed - 402.5) as speed,
    round(if((32.5*d5.speed - 402.5)<20,20,if((32.5*d5.speed - 402.5) > 150,150,(32.5*d5.speed - 402.5)))) as calculated_speed,
    d5.animation
  from 

    (select
      event_date,
      event_timestamp,
      user_pseudo_id,
      string_value as room_name,
      string_value_a2 as response_status
    from 
      base_table
    where 
      event_name in ('defended_event')
      and key in ('room_name')
      and key_a2 in ('response_status')
    group by 
      1,2,3,4,5
    ) as d1

  join 

    (select
      event_timestamp,
      user_pseudo_id,
      double_value as Xballcord,
      double_value_a2 as Yballcord
    from 
      base_table
    where 
      event_name in ('defended_event')
      and key in ('BallFinalPositionXCoordinate')
      and key_a2 in ('BallFinalPositionYCoordinate')
    group by 
      1,2,3,4
    ) as d2

  on 
    d1.user_pseudo_id = d2.user_pseudo_id
    and d1.event_timestamp = d2.event_timestamp
  join 

    (select
      event_timestamp,
      user_pseudo_id,
      int_value as swipe_direction,
      int_value_a2 as round
    from 
      base_table
    where 
      event_name in ('defended_event')
      and key in ('swipe_direction')
      and key_a2 in ('opponent_round_count')
    group by 
      1,2,3,4
    ) as d3

  on 
    d1.user_pseudo_id = d3.user_pseudo_id
    and d1.event_timestamp = d3.event_timestamp

  join 

    (select
      event_timestamp,
      user_pseudo_id,
      string_value as opp_id,
      string_value_a2 as status
    from 
      base_table
    where 
      event_name in ('defended_event')
      and key in ('opp_id')
      and key_a2 in ('status')
    group by 
      1,2,3,4
    ) as d4

  on 
    d1.user_pseudo_id = d4.user_pseudo_id
    and d1.event_timestamp = d4.event_timestamp

   join 

    (select
      event_timestamp,
      user_pseudo_id,
      double_value as speed,
      int_value_a2 as animation
    from 
      base_table
    where 
      event_name in ('defended_event')
      and key in ('velocity')
      and key_a2 in ('animation_type')
    group by 
      1,2,3,4
    ) as d5

  on 
    d1.user_pseudo_id = d5.user_pseudo_id
    and d1.event_timestamp = d5.event_timestamp

  ) as d

on
  b.user_pseudo_id = d.user_pseudo_id
  and b.event_date = d.event_date

LEFT JOIN
	
  (select
    e1.event_date,
    e1.event_timestamp,
    e1.user_pseudo_id,
    e1.room_name,
    e1.round,
    e2.Xstartcord,
    e2.Ystartcord,
    e3.Xendcord,
    e3.Yendcord,
    (atan2(e3.Yendcord-e2.Ystartcord,e3.Xendcord-e2.Xstartcord) * 180/acos(-1)) as swipe_angle,
    case
    	when (atan2(e3.Yendcord-e2.Ystartcord,e3.Xendcord-e2.Xstartcord) * 180/acos(-1)) <= 0 and (atan2(e3.Yendcord-e2.Ystartcord,e3.Xendcord-e2.Xstartcord) * 180/acos(-1)) >= -90 then 180.0
    	when (atan2(e3.Yendcord-e2.Ystartcord,e3.Xendcord-e2.Xstartcord) * 180/acos(-1)) < -90 then 0.0
    	else 180 - (atan2(e3.Yendcord-e2.Ystartcord,e3.Xendcord-e2.Xstartcord) * 180/acos(-1))
   	end as calculated_swipe_angle,
    e4.swipe_time,
    e5.draw_end_time,
    e5.draw_end_time - e4.swipe_time as  response_time,
    e4.flight_time,
    0.3 * e4.flight_time as t0_30,
    0.7 *e4.flight_time as t0_70,
    round(e5.draw_end_time/e4.flight_time,1) as fraction	
  from 

    (select
      event_date,
      event_timestamp,
      user_pseudo_id,
      string_value as room_name,
      int_value_a2 as round
    from 
      base_table
    where 
      event_name in ('keeper_swipe_event')
      and key in ('room_name')
      and key_a2 in ('opponent_round_count')
    group by 
      1,2,3,4,5
    ) as e1

  join 

    (select
      event_timestamp,
      user_pseudo_id,
      double_value as Xstartcord,
      double_value_a2 as Ystartcord
    from 
      base_table
    where 
      event_name in ('keeper_swipe_event')
      and key in ('start_x')
      and key_a2 in ('start_y')
    group by 
      1,2,3,4
    ) as e2

  on 
    e1.user_pseudo_id = e2.user_pseudo_id
    and e1.event_timestamp = e2.event_timestamp

  join 

    (select
      event_timestamp,
      user_pseudo_id,
      double_value as Xendcord,
      double_value_a2 as Yendcord
    from 
      base_table
    where 
      event_name in ('keeper_swipe_event')
      and key in ('end_x')
      and key_a2 in ('end_y')
    group by 
      1,2,3,4
    ) as e3

  on 
    e1.user_pseudo_id = e3.user_pseudo_id
    and e1.event_timestamp = e3.event_timestamp

  join 

    (select
      event_timestamp,
      user_pseudo_id,
      double_value as swipe_time,
      double_value_a2 as flight_time
    from 
      base_table
    where 
      event_name in ('keeper_swipe_event')
      and key in ('swipe_duration')
      and key_a2 in ('flight_time')
    group by 
      1,2,3,4
    ) as e4

  on 
    e1.user_pseudo_id = e4.user_pseudo_id
    and e1.event_timestamp= e4.event_timestamp

   join 

    (select
      event_timestamp,
      user_pseudo_id,
      double_value as draw_end_time
    from 
      base_table
    where 
      event_name in ('keeper_swipe_event')
      and key in ('draw_time_taken')
    group by 
      1,2,3
    ) as e5

  on 
    e1.user_pseudo_id = e5.user_pseudo_id
    and e1.event_timestamp = e5.event_timestamp

  ) as e

on
  d.user_pseudo_id = e.user_pseudo_id
  and d.event_date = e.event_date
  and d.room_name = e.room_name
  and d.round = e.round

LEFT JOIN

	(SELECT
      event_date,
      event_timestamp,
      user_pseudo_id,
      string_value as room_name,
      int_value_a2 as goalkeeping_attribute
    FROM
      base_table
    WHERE
    	event_name in ('game_play_started_event')
    	and key in ('room_name')
    	and key_a2 in ('goalkeeping')
    GROUP BY
      1,2,3,4,5
  	) as f
on
  d.user_pseudo_id = f.user_pseudo_id
  and d.room_name = f.room_name
  and d.event_date = f.event_date

LEFT JOIN
	
	(SELECT
      g1.event_date,
      g1.user_pseudo_id,
      g1.room_name as room_name,
      g1.round,
      g2.curve
    FROM
      (select
      event_date,
      event_timestamp,
      user_pseudo_id,
      string_value as room_name,
      int_value_a2 as round
    from 
      base_table
    where 
      event_name in ('opposition_kicked_event')
      and key in ('room_name')
      and key_a2 in ('player_round_count')
    group by 
      1,2,3,4,5
    ) as g1

  join 

    (select
      event_timestamp,
      user_pseudo_id,
      double_value as curve
    from 
      base_table
    where 
      event_name in ('opposition_kicked_event')
      and key in ('curve')
    group by 
      1,2,3
    ) as g2

  on 
    g1.user_pseudo_id = g2.user_pseudo_id
    and g1.event_timestamp = g2.event_timestamp
  	) as g
on
  d.user_pseudo_id = g.user_pseudo_id
  and d.room_name = g.room_name
  and d.event_date = g.event_date	
  and d.round = g.round

WHERE
  d.event_timestamp is not null
