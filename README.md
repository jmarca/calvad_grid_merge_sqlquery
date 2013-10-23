# CalVAD grid merge

So I have data by 4km grid in two sets, one based on detectors, the
other for HPMS data, but just the hourly fraction of AADT.  This code
merges those two to generate higher sized areas (county, etc) by hour,
of VMT, etc.

Eventually it may also produce vmt by grid cell, but not at the moment
as an output, only as an intermediate step, because saving that takes
up too much space, and should be pretty quick to compute on a one by
one basis.
