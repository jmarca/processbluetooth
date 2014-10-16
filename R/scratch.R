library(testthat)
library(RPostgreSQL)
library(stats)
library(plyr)

library(ggplot2)



## load data
## later, need to namespace this

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

limited.query <- paste(
    "with observed as (",
    "    select distinct", "numtrips,traveltime,segmentid,ts,radar_lane_id,station_lane_id,from_name,to_name",
    "    from smartsig.bt_xml_and_data",
    "    where segmentid in (4555,4556)",
    "    and numtrips>0 and traveltime>0",
    ")",
    "select * from observed"
)
stupid.query <- 'select * from smartsig.bt_xml_and_data'

rs <- dbSendQuery(con, limited.query)

df.bt.data <- fetch(rs,n=-1)

## southbound section is sectionid 4555
bt.southbound.idx <- df.bt.data$segmentid==4555
## northbound section is sectionid 4556
bt.northbound.idx <- df.bt.data$segmentid==4556


expect_that(
    levels(as.factor(df.bt.data$to_name[bt.northbound.idx]))
   ,equals(c("SR-39 at Hillsborough Dr")))
expect_that(
    levels(as.factor(df.bt.data$from_name[bt.northbound.idx]))
   ,equals(c("SR-39 at La Mirada Blvd")))

expect_that(
    levels(as.factor(df.bt.data$to_name[bt.southbound.idx]))
   ,equals(c("SR-39 at La Mirada Blvd")))
expect_that(
    levels(as.factor(df.bt.data$from_name[bt.southbound.idx]))
   ,equals(c("SR-39 at Hillsborough Dr")))


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

## t01001_201407 (northbound)
q <- 'select * from smartsig_beach.t01001_201407'
rs <- dbSendQuery(con,q)
t01001_201407 <- fetch(rs,n=-1)

## t01002_201407 (southbound)
q <- 'select * from smartsig_beach.t01002_201407'
rs <- dbSendQuery(con,q)
t01002_201407 <- fetch(rs,n=-1)


dbDisconnect(con)

library(plyr)


dow.levels <- c(
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday"
)



## timestamp is grouper, from craig's code

sst.aggregator <- function(d){
    aggregated <- ddply(d, .(timestamp), summarize,
                        traveltime = sum(travel_time),
                        timestamp = timestamp[1])

    ## fix timestamp...from Craig's code

    ## according to Craig, the hour is off by one
    ## but which way?  UTC - 9 should be UTC - 8, or UTC - 7??
    ## So add one, or add two?
    ##
    now <- as.POSIXlt(strptime(
        paste("20",aggregated$timestamp,sep=""),
        "%Y%m%d%H%M%S"))
    now$hour <- now$hour + 1
    ## mask the edit
    then <- as.POSIXlt(as.POSIXct(now))
    aggregated$hr <- as.factor(then$hour)

    aggregated$dow <- factor(weekdays(as.POSIXct(then)),
                             levels=dow.levels)


    aggregated
}

sst.north.aggregated <- sst.aggregator(t01001_201407)
sst.south.aggregated <- sst.aggregator(t01002_201407)



## ditto for bluetooth data

df.bt.data$dow <- factor(weekdays(as.POSIXct(df.bt.data$ts)),levels=dow.levels)
df.bt.data$hr  <- as.factor(as.POSIXlt(df.bt.data$ts)$hour)


## now, apply tests to see if the two distributions of end to end
## travel times are roughly the same thing


## older stuff
##
## mean.sd <- ddply(t01001.aggregated, .(dow,hr), summarize,
##                  mean = round(mean(travel_time), 2),
##                  sd = round(sd(travel_time), 2),
##                  min = min(travel_time),
##                  max = max(travel_time)
##                  )



## bt data exploration

df.bt.data$weight <- 1

df.bt.data$weight[bt.southbound.idx] <- df.bt.data$numtrips[bt.southbound.idx]/sum(df.bt.data$numtrips[bt.southbound.idx])

df.bt.data$weight[bt.northbound.idx] <- df.bt.data$numtrips[bt.northbound.idx]/sum(df.bt.data$numtrips[bt.northbound.idx])


bt.histogram.function <- function(d,w=NULL){
    m <- ggplot(d, aes(x=traveltime))
    if(!is.null(w)){
        m <- ggplot(d, aes_string(x="traveltime",weight=w))
    }
    m + geom_histogram(binwidth=10,aes(y = ..density..,fill=..density..))
}

## southbound histogram
bt.histogram.function(df.bt.data[bt.southbound.idx,],w='numtrips')

## northbound histogram
## bt.histogram.function(df.bt.data[bt.northbound.idx,])

bt.histogram.function(df.bt.data[bt.northbound.idx,],w='numtrips')


## temporary testing code
df.bt <- df.bt.data[bt.northbound.idx,]

df.bt <- df.bt.data[bt.southbound.idx,]

## m <- ggplot(df.bt, aes(x=traveltime
##                        ))

## m + geom_histogram(binwidth=20,
##                    aes(y = ..density..
##                       ,weight=numtrips
##                       ,fill=..density..))+
##                           geom_density() +
##                               facet_wrap( ~ dow )

## m + geom_histogram(binwidth=20,aes(y = ..density..,fill=..density..))+geom_density() + facet_wrap( ~ hr )



## similar thing for sst data??

outlier.north.idx <- sst.north.aggregated$traveltime<600
outlier.south.idx <- sst.south.aggregated$traveltime<600

## m <- ggplot(sst.north.aggregated[outlier.north.idx,], aes(x=traveltime
##                        ))

## m + geom_histogram(binwidth=20,
##                    aes(y = ..density..
##                       ,fill=..density..))+
##                           geom_density() +
##                               facet_wrap( ~ dow )

## m + geom_histogram(binwidth=20,aes(y = ..density..,fill=..density..))+geom_density() + facet_wrap( ~ hr )


## bt.histogram.function(sst.north.aggregated[outlier.north.idx,])

## bt.histogram.function(sst.south.aggregated[outlier.south.idx,])

## for the ks test, need to weight the travel times by the number of observations

## nb.weighted.times <- rep(x=df.bt.data[bt.northbound.idx,'traveltime'],
##                          times=df.bt.data[bt.northbound.idx,'numtrips'])

## expect_that(weighted.mean(
##     x=df.bt.data[bt.northbound.idx,'traveltime'],
##     w=df.bt.data[bt.northbound.idx,'numtrips'])
##            ,equals(mean(nb.weighted.times)))

## sb.weighted.times <- rep(x=df.bt.data[bt.southbound.idx,'traveltime'],
##                          times=df.bt.data[bt.southbound.idx,'numtrips'])

## expect_that(weighted.mean(
##     x=df.bt.data[bt.southbound.idx,'traveltime'],
##     w=df.bt.data[bt.southbound.idx,'numtrips'])
##            ,equals(mean(sb.weighted.times)))


## ks.test(nb.weighted.times,sb.weighted.times)
## ## definitely different distributions

## ks.test(nb.weighted.times,sst.north.aggregated[outlier.north.idx,'traveltime'])
## ## definitely different distributions


ks.test.fn <- function(bt.sst.data,title='ecdf plot',field=NULL){
    ## expect to be used in plyr
    ## so data is smashed together with flag 'type' to distinguish

    sst.times <- bt.sst.data$traveltime[bt.sst.data$type=='SST']

    bt.type <- bt.sst.data$type=='bluetooth'

    ## spin out weighted vector from btdata

    bt.weighted.times <- rep(x= bt.sst.data[bt.type,'traveltime'],
                         times=bt.sst.data[bt.type,'numtrips'])

    ## quick diagnostic plot
    df <- data.frame(traveltime=bt.weighted.times,type='bluetooth')
    df <- rbind(df,data.frame(traveltime=sst.times,type='SST'))

    p <- ggplot(df, aes(traveltime, colour = type)) + stat_ecdf() +
              scale_color_manual(values = c("blue", "red")) +
                  labs(title=title) +
                   ylab("Empirical cumulative density")
    if(! is.null(field)){
        p <- p + labs(title=paste(title,bt.sst.data[1,field],sep=': '))
    }
    print(p)

    res <- ks.test(bt.weighted.times,sst.times)
    return (res)
}

## limit to less than 600s trips
normal.trips.bt.nor <- bt.northbound.idx & df.bt.data$traveltime < 600
normal.trips.sst.nor <- sst.north.aggregated$traveltime < 600
normal.trips.bt.sou <- bt.southbound.idx & df.bt.data$traveltime < 600
normal.trips.sst.sou <- sst.south.aggregated$traveltime < 600


## combine, then split
bt.sst.n <- rbind(data.frame(traveltime=df.bt.data$traveltime[normal.trips.bt.nor]
                            ,type='bluetooth'
                            ,hr=df.bt.data$hr[normal.trips.bt.nor]
                            ,dow=df.bt.data$dow[normal.trips.bt.nor]
                            ,numtrips=df.bt.data$numtrips[normal.trips.bt.nor]
                             ),
                  data.frame(traveltime=sst.north.aggregated$traveltime[normal.trips.sst.nor]
                            ,type='SST'
                            ,hr=sst.north.aggregated$hr[normal.trips.sst.nor]
                            ,dow=sst.north.aggregated$dow[normal.trips.sst.nor]
                            ,numtrips=1)
                  )

bt.sst.s <- rbind(data.frame(traveltime=df.bt.data$traveltime[normal.trips.bt.sou]
                            ,type='bluetooth'
                            ,hr=df.bt.data$hr[normal.trips.bt.sou]
                            ,dow=df.bt.data$dow[normal.trips.bt.sou]
                            ,numtrips=df.bt.data$numtrips[normal.trips.bt.sou]
                             ),
                  data.frame(traveltime=sst.south.aggregated$traveltime[normal.trips.sst.sou]
                            ,type='SST'
                            ,hr=sst.south.aggregated$hr[normal.trips.sst.sou]
                            ,dow=sst.south.aggregated$dow[normal.trips.sst.sou]
                            ,numtrips=1)
                  )

expect_that(max(bt.sst.n$traveltime),is_less_than(600))
expect_that(max(bt.sst.s$traveltime),is_less_than(600))


## test "scale_color_manual"
## m <- ggplot(bt.sst.n, aes(traveltime, colour = type)) + stat_ecdf() +
##     scale_color_manual(values = c("blue", "red")) +
##         labs(title = "New plot title")

pdf("ks_ecdf.pdf", width = 8, height = 4)

res <- ks.test.fn(bt.sst.n,'Northbound, cumulative')

res <- ks.test.fn(bt.sst.s,'Southbound, cumulative')

dev.off()

pdf("ks_dow_ecdf.pdf", width = 8, height = 4)

res <- dlply(bt.sst.n,.(dow),ks.test.fn, 'Northbound, by day of week','dow')
res <- dlply(bt.sst.s,.(dow),ks.test.fn, 'Southbound, by day of week','dow')

dev.off()

pdf("ks_hr_ecdf.pdf", width = 8, height = 4)

res <- dlply(bt.sst.n,.(hr),ks.test.fn, 'Northbound, by hour of day','hr')
res <- dlply(bt.sst.s,.(hr),ks.test.fn, 'Southbound, by hour of day','hr')

dev.off()
