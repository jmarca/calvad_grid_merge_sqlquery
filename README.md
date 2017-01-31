# The postgresql queries for CalVAD grid merge

This is code related to postgresql queries only, used in grid_merge.

The idea of grid_merge is to combine the hourly fractions data from
couchdb with each of the grid's hpms roads, as pulled from postgresql.

This is the postgresql part of that process.

I am splitting out to clean up, modularize, etc.

# how to generate hpmsYYYY.json files

I used to use the grid_display web server to generate the
hpmsYYYY.json files (hpms2015.json, etc), but really it was calling
the code here.  Because we had an issue rolling that function out at
ARB, I wanted to trim it down to a minimal bit of code to simplify
debugging.

So now you can generate the hpms year json files here.

These files are the hpms data for a year aggregated up to the grid
level. Every grid cell with any sort of hpms data at all is included.
If the grid cell's HPMS data include roads that are actually covered
by detectors in the detector data set, then those roads are removed.

So there are two queries to PostgreSQL per grid cell.  The first
fetches all of the HPMS data.  The second fetches all of the
roadway segments in the grid with detector data.  Assuming there is
HPMS and detector data for the grid, then the segments that are *in*
both sets are removed from the HPMS set.  That is, the detector data
wins, HPMS loses, because the detector data already has hourly
variations, whereas the HPMS data has just a single AADT value.

This is done by comparing roadway names.  If there is a highway 101
anda  highway 103 in HPMS in a grid, and if there is also a highway
101 but not a highway 103 in the detector data for that grid, then the
highway 101 data for the grid HPMS data is discarded, whereas the
highway 103 data is kept.

Read the code in the `./lib` directory for more details.

The program `app.js` actually does this.  It requires a basic
config.json to work properly.  This should look something like:

```
{
    "postgresql":{
        "username":"mydbuser",
        "host":"127.0.0.1",
        "port":5432,
        "grid_merge_sqlquery_db":"hpmsdatabase"
    }
}
```

Change the variables to reflect where you put the hpms data and where
you put the OSM data.  On a server, the value for
`grid_merge_sqlquery_db` is "spatialvds".  For a test case, the value
might be "hpmstesting".

Make sure this file is called "config.json" and that the permissions
are correct:

```
chmod 0600 config.json
```


Next load the prerequisite libraries by running

```
npm install
```

Finally, at this point you should be able to run the program

```
node app.js
```

Without arguments, this will spit out a help message.  The only
requirement is that you give it a year to process.  You can also limit
the number of simultaneous PostgreSQL connections by passing the
parameter jobs.  For example:

```
node app.js -j 2 -y 2014
```

Note that there will be 2 times as many PostgreSQL connections as
specified above, because each parallel grid job will connect once for
HPMS data, and once for highway detector data.

On a recent model laptop, this task takes about 1 hour to process
2014.




# The individual files

The tests are decent.  Read them for more clue on functionality.

## `query_postgres.js`

query postgres hits the database and gets AADT and weights by length
of roadway.  it does this for all roads in the HPMS data inside of the
grid cell.  It does not yet allow for the floating,  non-assigned hpms
records (those that do not have a geometry and therefore do not yet
have grid cell membership).  The sql query is decent, and tested and
used.  I had a big issue developing it and so made lots of tests adn
things over time.

## `query_postgres_2014.js`

Same as `query_postgres.js`, except the queries have been changed to use
the newer format of the HPMS data since the 2014 data was released.
Note that this version of the functions in  `query_postgres.js` is
used for any year greater than 2009 (the last year for which we have
good data in the older (more complete) HPMS format.  The post 2009
data is not nearly as rich as the 2007, 2008, 2009 data, but Caltrans
is unwilling to provide the complete HPMS data for some reason.
Specifically, this data no longer has any information on lanes, and so
the lane-miles in the output is zero.



## `f_system.json`

hardcode the functional classification system used by caltrans in
their hpms data

## reduce.js

Code to mix and match and massage the hpms and detector based data.
For example, if a cell has a freeway with data in both the hpms and
detector-based data sets, then usually you want to drop it from the
hpms data and keep the detector based one.  So that's what this
does...it looks through the detector based query from sql database and
lists off the freeways, and then looks through the HPMS data and gets
rid of freeways that are duplicates.

The test of the reduce code shows this, and the teest of the routes
shows this super explicitly, with one case not using the detector
data, and the other using it and getting back only 3 records, not 5.

## routes.js

two convenience routes for a web server.  Not sure how to use them yet
on the client end, so they'll probably change, but at the moment the
first one just does what the grid_merge app.js does...get the hpms and
the detectors in the grid, then does the sql post processing to merge
those two bits of information.

The other one is even easier in that it does not go get the
detector-based data.

# testing caveat

The tests basically checks to make sure that things are working right,
but they also expect that the db has certain data already.  If the
data changes or is not there, the test will fail.  Again, as usual, if
you are not James Marca you should make sure you know what is going on
before you start using this code.
