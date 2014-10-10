
## load data
## later, need to namespace this

library(RPostgreSQL)
m <- dbDriver("PostgreSQL")
psqlenv <- Sys.getenv(c("PGHOST", "PGUSER", "PGPASSWORD", "PGDATABASE","PGPORT"))

host=psqlenv[1]
user=psqlenv[2]
password=psqlenv[3]
dbname=psqlenv[4]
port=psqlenv[5]

con <-  dbConnect(m
                 ,user=user
                 ,password=password
                 ,host=host
                 ,port=port
                 ,dbname=dbname)

rs <- dbSendQuery(con, 'select * from smartsig.bt_xml_and_data')

df.bt.data <- fetch(rs,n=-1)


## load the sst tables

q <- 'select * from smartsig_beach.d0101_201407'
rs <- dbSendQuery(con,q)
d0101_201407 <- fetch(rs,n=-1)

q <- 'select * from smartsig_beach.d0103_201407'
rs <- dbSendQuery(con,q)
d0103_201407  <- fetch(rs,n=-1)

q <- 'select * from smartsig_beach.d0104_201407'
rs <- dbSendQuery(con,q)
d0104_201407  <- fetch(rs,n=-1)
q <- 'select * from smartsig_beach.d0105_201407'
rs <- dbSendQuery(con,q)
d0105_201407  <- fetch(rs,n=-1)
q <- 'select * from smartsig_beach.d0106_201407'
rs <- dbSendQuery(con,q)
d0106_201407  <- fetch(rs,n=-1)
q <- 'select * from smartsig_beach.detector_info'
rs <- dbSendQuery(con,q)
detector_info <- fetch(rs,n=-1)
q <- 'select * from smartsig_beach.q0101_201407'
rs <- dbSendQuery(con,q)
q0101_201407  <- fetch(rs,n=-1)
q <- 'select * from smartsig_beach.q0103_201407'
rs <- dbSendQuery(con,q)
q0103_201407  <- fetch(rs,n=-1)
q <- 'select * from smartsig_beach.q0104_201407'
rs <- dbSendQuery(con,q)
q0104_201407  <- fetch(rs,n=-1)
q <- 'select * from smartsig_beach.q0105_201407'
rs <- dbSendQuery(con,q)
q0105_201407  <- fetch(rs,n=-1)
q <- 'select * from smartsig_beach.q0106_201407'
rs <- dbSendQuery(con,q)
q0106_201407  <- fetch(rs,n=-1)
q <- 'select * from smartsig_beach.s0101_201407'
rs <- dbSendQuery(con,q)
s0101_201407  <- fetch(rs,n=-1)
q <- 'select * from smartsig_beach.s0103_201407'
rs <- dbSendQuery(con,q)
s0103_201407  <- fetch(rs,n=-1)
q <- 'select * from smartsig_beach.s0104_201407'
rs <- dbSendQuery(con,q)
s0104_201407  <- fetch(rs,n=-1)
q <- 'select * from smartsig_beach.s0105_201407'
rs <- dbSendQuery(con,q)
s0105_201407  <- fetch(rs,n=-1)
q <- 'select * from smartsig_beach.s0106_201407'
rs <- dbSendQuery(con,q)
s0106_201407  <- fetch(rs,n=-1)
q <- 'select * from smartsig_beach.segment_info'
rs <- dbSendQuery(con,q)
segment_info  <- fetch(rs,n=-1)
q <- 'select * from smartsig_beach.t01001_201407'
rs <- dbSendQuery(con,q)
t01001_201407 <- fetch(rs,n=-1)
q <- 'select * from smartsig_beach.t01002_201407'
rs <- dbSendQuery(con,q)
t01002_201407 <- fetch(rs,n=-1)


dbDisconnect(con)

library(plyr)



## timestamp is grouper, from craig's code

t101001.aggregated <- ddply(t01001_201407, .(timestamp), summarize,
                            travel_time = sum(travel_time),
                            timestamp = timestamp[1])



## fix timestamp...from Craig's code

t101001.aggregated$dow <- as.factor(weekdays(as.POSIXct(strptime(
        paste("20",t101001.aggregated$timestamp,sep=""),
    "%Y%m%d%H%M%S"))))

t101001.aggregated$hr <-
    as.factor((as.POSIXlt(strptime(
        paste("20",t101001.aggregated$timestamp,sep=""),
        "%Y%m%d%H%M%S")))$hour
              )

mean.sd <- ddply(t101001.aggregated, .(dow,hr), summarize,
                 mean = round(mean(travel_time), 2),
                 sd = round(sd(travel_time), 2),
                 min = min(travel_time),
                 max = max(travel_time)
                 )
