
=============================
Redis timeseries API
=============================

**An implementation of a timeseries API in redis_.**

Timeseries is an important data-structure not yet supported in redis,
it is represented by a unique sorted associative container,
that is to say it associates ordered unique times to values. 

Values, which can be anything you like, are ordered with respect to times (double values),
and can be accessed by times or rank (the order of times in the timeseries).
Times are unique, that is to say in a timeseries
there will be only one value associated with a time.

Internally, values are added to a hash table mapping them to times.
At the same time they are added to a skip list to maintain
sorting with respect to times.

Implementation is almost equivalent to zsets, and they look like zsets. But they are not zsets!

Performance::

	O(log(N)) on INSERT and REMOVE operations
	O(1) on RETRIEVAL via times

.. contents::
    :local:
    	
	
COMMANDS
================

 
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

	tsrange key start end <flag>
 
Where start and end are integers following the same
Redis conventions as zrange, <flag> is an optional
string which can take two values: ``withtimes`` or ``novalues``.
 
	tsrange key start end			-> return values
	tsrange key start end withtimes	-> return (time,value)s
	tsrange key start end novalues	-> return times
 
TSRANGEBYTIME
------------------
Range by times
 
	tsrangebyscore score_start score_end <flag>
 
TSCOUNT
------------------
Count element in range by ``time``::

	tscount time_start,time_end

 
SOURCE CODE CHANGES
==========================

I have tried as much as possible to be not intrusive so that it should be relatively straightforward to
add track changes. In a nutshell, these are the additions:

* Added 2 files, t_ts.h_ and t_ts.c_.
* Modified redis.c_ to add timeseries commands to the command table and added the ``ts_h`` include.
* Modified Makefile_ so that ``t_ts.c`` is compiled.


.. _redis: http://redis.io/
.. _Makefile: https://github.com/lsbardel/redis/blob/redis-timeseries/src/Makefile
.. _t_ts.c: https://github.com/lsbardel/redis/blob/redis-timeseries/src/t_ts.c
.. _t_ts.h: https://github.com/lsbardel/redis/blob/redis-timeseries/src/t_ts.h
.. _redis.c: https://github.com/lsbardel/redis/blob/redis-timeseries/src/redis.c
