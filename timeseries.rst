

Time Series API
=============================

This is a proposal implementation for a timeserie API in redis_.
A timeseries is Redis implementation of a unique sorted associative container which
uses two data structures to hold scores and values in order to obtain
O(log(N)) on INSERT and REMOVE operations and O(1) on RETRIEVAL via scores.

Values are ordered with respect to times (double values) same as zsets,
and can be accessed by times or rank.
The values are added to an hash table mapping these values to times.
At the same time the values are added to a skip list to maintain
sorting with respect times.
 
Implementation is almost equivalent to zsets.
The only caveat is the switching between scores(times) and members(values) in the hash table.
Small difference, but an important one.

 
TLEN
----------
Size of timeserie
 
  		tslen key
 
TSADD
---------------
Add items to timeserie::

	tsadd key time1 value1 time2 value2 ...
 
If value at time is already available, the value will be updated
 

TSEXISTS
------------------
Check if time is in timeserie
 
  		tsexists key time
 
TSGET
------
Get value at time

	tsget key score
 
TSRANGE
------------------
Range by rank in skiplist::

	trange key start end <flag>
 
Where start and end are integers following the same
Redis conventions as zrange, <flag> is an optional
string which can take two values: "withscores" or "novalues"
 
	trange key start end			-> return only values
	trange key start end withscores	-> return score,value
	trange key start end novalues	-> return score
 
TSRANGEBYTIME
------------------
Range by times
 
	trangebyscore score_start score_end <flag>
 
TSCOUNT
------------------
Count element in range by score::

	tcount score_start,score_end
 

_ redis: http://code.google.com/p/redis/