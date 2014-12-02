# get all of california at once

Copying the approach from grid_merge, what I want to do is to list off
all of the grid cells, then process each in turn, then combine for a
result.  I need to keep the grid_cell as well in the response.

What I need is grid_cell, roadway types, total across roadway types,
and that's all.  So I can color by a variety of values.

This is becoming an HPMS data browser tool, and that is okay.  One
thing I noticed is that I drop the FHWA links.  Why is that?

Testing the query to see what is going on there.

Yep.  Look at scratch_query.sql.  It shows that if you select FHWA
records, there are 573 of them, but none of them have records in the
hpms_link_geom table, which means they are not geocoded.

So that where clause is irrelevant.

Anyway, what I want to do is list all of the cells in the state, and
then one by one query all of them, and then spit out the results.

This seems wasteful.  Can't I just get all of them at once in the sql?
How slow is it?  Probably slow, but not necessarily that slow because
there are only a few tens of thousands of grids.  Actually, there are
only 42,108  HPMS records that are geocoded in 2009, so it is likely
even less.

Try it and see how expensive it is.

## logic of "get the whole state":

get all the grids in the state.

feed each to the sql query code

concatenate the results

return to client.

Ah, but I also need to remember to tag each record with its grid cell
