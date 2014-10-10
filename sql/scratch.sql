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
