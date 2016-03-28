# keep refactoring app.js

follow the logic path.  have detector freeways, have hpms groupings.
So remove the freeways that I have in calvad from the hpms totals

then do the pull from couchdb, and multiply the aadt by hourly factors

3. sql query for hpms not assigned geo inside county

4. sql query for grid count of assigned geo inside county

5. evenly distribute hpms not assigned to grids in county

6. modify 3 to include 5

7. save the cumulative sums to couchdb or something by county or something

----

# more refactoring, code to evaluate 2012 hpms vs 2014 hpms

What I need to do is select for each grid cell the difference in miles
(clipped_length) in 2012 partially geocoded versus 2014 geocoded.

My thought is that for each city the total length of roads should be
the same.  So what is that query?

Of course there will be a difference, so simply scale up for the
entire city?

How about length of roads that are not geocoded plus length of roads
that are geocoded to get a total length of roads in city.


Okay, more messy mess to deal with.  in hpms_data, there are many
abbreviations and so on.

but that is for 2009 and prior.  what about only 2012

# thinking

Okay.  looks like on my laptop, `hpms.hpms` in db `hpms_geocode` is the
more recent data.  It doesn't have exactly the same columns as the old
`hpms.hpms_data` table in spatialvds.  Also, some of the `route_id`
values are the new way, with `county_city_...` and I need to split out
the city part and pair with city abbreviations.

Other are clearly the old `route_id`, so I need to join with the old
data.
