 select station_lane_id,
sum(numtrips*traveltime)/sum(numtrips) from
smartsig.bt_xml_observation where segmentid in (4555,4556) group by
station_lane_id;


-- try to not multicount

with observed as (
     select distinct numtrips,traveltime,segmentid,ts,radar_lane_id,station_lane_id
     from smartsig.bt_xml_observation
     where segmentid in (4555,4556)
)
select station_lane_id,segmentid,sum(numtrips*traveltime)/sum(numtrips)
from observed
group by station_lane_id,segmentid;


with observed as (
     select distinct numtrips,traveltime,segmentid,ts,radar_lane_id,station_lane_id,from_name,to_name
     from smartsig.bt_xml_and_data
     where segmentid in (4555,4556)
     and numtrips>0 and traveltime>0
)
select * from observed;
