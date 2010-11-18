

Time Series API
=============================

This is a proposal for a possible implementation of a timeserie API in redis_.

A timeserie is an important data-structure not yet supported.
It is represented by a unique sorted associative container,
that is to say it associates ordered unique times to values. 

Values, which can be anything you like, are ordered with respect to times (double values),
and can be accessed by times or rank (the order of times in the timeserie).
Times are unique, that is to say in a timeserie
there will be only one value associated with a time.

Internally, values are added to a hash table mapping them to times.
At the same time they are added to a skip list to maintain
sorting with respect times.
Implementation is almost equivalent to zsets, and they look like zsets. But they are not.
The only caveat is the switching between scores(times) and members(values) in the hash table.

Performance::

	O(log(N)) on INSERT and REMOVE operations
	O(1) on RETRIEVAL via times

 
TSLEN
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

	tsget key time
 
TSRANGE
------------------
Range by rank in skiplist::

	trange key start end <flag>
 
Where start and end are integers following the same
Redis conventions as zrange, <flag> is an optional
string which can take two values: ``withtimes`` or ``novalues``.
 
	trange key start end			-> return values
	trange key start end withtimes	-> return (time,value)s
	trange key start end novalues	-> return times
 
TSRANGEBYTIME
------------------
Range by times
 
	trangebyscore score_start score_end <flag>
 
TSCOUNT
------------------
Count element in range by score::

	tcount score_start,score_end
	
	
TSUNION
-----------------------------------------
**still to decide what form the value will take**
Union ``N`` timeseries. If a series have missing times, ``NaN`` will be inserted::

	TSUNION key1, key2, ..., keyN
	
	
TSINTERCEPTION
-----------------------------------------
**still to decide what form the value will take**
Merge ``N`` by performing an interception of times::

	TSUNION key1, key2, ..., keyN
 

.. _redis: http://code.google.com/p/redis/
