# CalVAD grid merge; sql query edition

This is code related to postgresql queries only, used in grid_merge.

I am splitting out to use elsewhere, and to clean up, modularize, etc.

Not yet actually used in grid merge


# hwat it is

The tests are decent.

## query_postgres.js

query postgres hits the database and gets AADT and weights by length
of roadway.  it does this for all roads in the HPMS data inside of the
grid cell.  It does not yet allow for the floating,  non-assigned hpms
records (those that do not have a geometry and therefore do not yet
have grid cell membership).  The sql query is decent, and tested and
used.  I had a big issue developing it and so made lots of tests adn
things over time.

## f_system.json

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
