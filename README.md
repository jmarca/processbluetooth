# Process Bluetooth

This is process bluetooth data from Caltrans bluetooth detectors.
Mostly R code.  Pull data from database into R, process, dump stats.

That's the idea, anyway.

[![Build Status](https://travis-ci.org/jmarca/processbluetooth.png?branch=master)](https://travis-ci.org/jmarca/processbluetooth)


# functions

As I think about this

I want to get all the data from the db, using read.ogr code perhaps,
except that the bluetooth data doesn't naturally have geo coordinates,
right?  and then i want t have functions in this package that can
operate on some items of records.

Then the idea is that in the analysis package, I can write R scripts
that use the functions from this package to run on subsets of the
records from the table, via plyr.

This way I can have code and tests for the analysis functions, but not
have the messy analysis stuff here in this repo.
