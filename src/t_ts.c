#include "t_ts.h"

#include <math.h>


/*-----------------------------------------------------------------------------
 * timeseries commands
 *----------------------------------------------------------------------------*/

void tslenCommand(redisClient *c) {
    robj *o;
    zset *mp;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_TS)) return;

    mp = o->ptr;
    addReplyLongLong(c,mp->zsl->length);
}


void tsexistsCommand(redisClient *c) {
    robj *o;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_TS)) return;

    addReply(c, tsTypeExists(o,c->argv[2]) ? shared.cone : shared.czero);
}


void tsgetCommand(redisClient *c) {
	robj *o, *value;
	if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk)) == NULL ||
		checkType(c,o,REDIS_TS)) return;

	if ((value = tsTypeGet(o,c->argv[2])) != NULL) {
		addReplyBulk(c,value);
		decrRefCount(value);
	} else {
		addReply(c,shared.nullbulk);
	}
}


void tsaddCommand(redisClient *c) {
	int i;
	double timeval;
	robj *o;

	if ((c->argc % 2) == 1) {
		addReplyError(c,"wrong number of arguments for TADD");
		return;
	}

	if ((o = tsTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
	hashTypeTryConversion(o,c->argv,2,c->argc-1);
	for (i = 2; i < c->argc; i += 2) {
		if(getDoubleFromObjectOrReply(c,c->argv[i],&timeval,NULL) != REDIS_OK) return;
	    tsTypeSet(o,timeval,c->argv[i],c->argv[i+1]);
	}
	addReply(c, shared.ok);
	touchWatchedKey(c->db,c->argv[1]);
	server.dirty++;
}


void tsrangeCommand(redisClient *c) {
	long start = 0;
	long end = 0;
	int withvalues = 1;
	int withtimes = 0;
	if ((getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != REDIS_OK) ||
		(getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != REDIS_OK)) return;
	tsrangeRemaining(c,&withtimes,&withvalues);
	tsrangeGenericCommand(c,start,end,withtimes,withvalues,0);
}


void tsrangebytimeCommand(redisClient *c) {
	tsrangebytimeGenericCommand(c,0,0);
}

void tscountCommand(redisClient *c) {
	tsrangebytimeGenericCommand(c,0,1);
}


/*-----------------------------------------------------------------------------
 * Internals
 *----------------------------------------------------------------------------*/

robj *createTsObject(void) {
	zset *zs = zmalloc(sizeof(*zs));

    zs->dict = dictCreate(&zsetDictType,NULL);
    zs->zsl = zslCreate();
    return createObject(REDIS_TS,zs);
}


robj *tsTypeLookupWriteOrCreate(redisClient *c, robj *key) {
    robj *o = lookupKeyWrite(c->db,key);
    if (o == NULL) {
        o = createTsObject();
        dbAdd(c->db,key,o);
    } else {
        if (o->type != REDIS_TS) {
            addReply(c,shared.wrongtypeerr);
            return NULL;
        }
    }
    return o;
}


/* Test if the key exists in the given ts. Returns 1 if the key
 * exists and 0 when it doesn't. */
int tsTypeExists(robj *o, robj *time) {
	zset *mp = o->ptr;
    if (dictFind(mp->dict,time) != NULL) {
    	return 1;
    }
    return 0;
}


/* Get the value from a hash identified by key. Returns either a string
 * object or NULL if the value cannot be found. The refcount of the object
 * is always increased by 1 when the value was found. */
robj *tsTypeGet(robj *o, robj *time) {
    robj *value = NULL;
    zset *mp = o->ptr;
	dictEntry *de = dictFind(mp->dict,time);
	if (de != NULL) {
		value = dictGetEntryVal(de);
		incrRefCount(value);
	}
    return value;
}

/* Add an element, discard the old if the key already exists.
 * Return 0 on insert and 1 on update. */
int tsTypeSet(robj *o, double timeval, robj *time, robj *value) {
	int update = 0;
    robj *ro;
    dictEntry *de;
    zset *mp;
    zskiplistNode *ln;
    mp = o->ptr;

    de = dictFind(mp->dict,time);
    if(de) {
    	/* time is available.*/

    	//Get the element in skiplist
    	ln = zslFirstWithScore(mp->zsl,timeval);
    	redisAssert(ln != NULL);

    	// Update the member in the hash
    	ro = dictGetEntryVal(de);
    	decrRefCount(ro);
    	dictGetEntryVal(de) = value;
    	incrRefCount(value); /* for dict */

    	//Update member in skiplist
    	ro = ln->obj;
    	decrRefCount(ro);
    	ln->obj = value;
    	incrRefCount(value); /* for dict */
    } else {
    	/* New element */
        ln = zslInsert(mp->zsl,timeval,value);
        incrRefCount(value); /* added to skiplist */

        /* Update the time in the dict entry */
        dictAdd(mp->dict,time,value);
        incrRefCount(time); /* added to hash */
        incrRefCount(value); /* added to hash */
        update = 1;
    }
    return update;
}


void tsrangeRemaining(redisClient *c, int *withtimes, int *withvalues) {
	/* Parse optional extra arguments. Note that ZCOUNT will exactly have
		 * 4 arguments, so we'll never enter the following code path. */
	if (c->argc > 4) {
		int remaining = c->argc - 4;
		int pos = 4;

		while (remaining) {
			if (remaining >= 1 && !strcasecmp(c->argv[pos]->ptr,"withtimes")) {
				pos++; remaining--;
				*withtimes = 1;
			} else if (remaining >= 1 && !strcasecmp(c->argv[pos]->ptr,"novalues")) {
				pos++; remaining--;
				*withvalues = 0;
				*withtimes = 1;
			} else {
				addReply(c,shared.syntaxerr);
				return;
			}
		}
	}
	if(*withtimes + *withvalues == 0)
		*withvalues = 1;
}


void tsrangeGenericCommand(redisClient *c, int start, int end, int withtimes, int withvalues, int reverse) {
    robj *o;
    int llen;
    int rangelen, j;
    zset *mp;
    zskiplist *zsl;
    zskiplistNode *ln;
    robj *value;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptymultibulk)) == NULL
             || checkType(c,o,REDIS_TS)) return;
	mp   = o->ptr;
	zsl  = mp->zsl;
	llen = zsl->length;

	if (start < 0) start = llen+start;
	if (end < 0) end = llen+end;
	if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    if (start > end || start >= llen) {
        addReply(c,shared.emptymultibulk);
        return;
    }
    if (end >= llen) end = llen-1;
    rangelen = (end-start)+1;

    /* check if starting point is trivial, before searching
     * the element in log(N) time */
    if (reverse) {
        ln = start == 0 ? zsl->tail : zslistTypeGetElementByRank(zsl, llen-start);
    } else {
        ln = start == 0 ?
            zsl->header->level[0].forward : zslistTypeGetElementByRank(zsl, start+1);
    }

    /* Return the result in form of a multi-bulk reply */
    addReplyMultiBulkLen(c,rangelen*(withtimes+withvalues));
    for (j = 0; j < rangelen; j++) {
        if (withtimes)
        	addReplyDouble(c,ln->score);
        if (withvalues) {
        	value = ln->obj;
        	incrRefCount(value);
        	addReplyBulk(c,value);
        }
        ln = reverse ? ln->backward : ln->level[0].forward;
    }
}



void tsrangebytimeGenericCommand(redisClient *c, int reverse, int justcount) {
	zrangespec range;
	zset *mp;
	zskiplist *zsl;
	zskiplistNode *ln;
	robj *o, *value, *emptyreply;
	int withvalues = 1;
	int withtimes = 0;
	unsigned long rangelen = 0;
	void *replylen = NULL;

	/* No reverse implementation for now */
	if(reverse) {
	    addReplyError(c,"No reverse implementation");
	    return;
	}

	/* Parse the range arguments. */
	if (zslParseRange(c->argv[2],c->argv[3],&range) != REDIS_OK) {
		addReplyError(c,"min or max is not a double");
		return;
	}
	tsrangeRemaining(c,&withtimes,&withvalues);

	/* Ok, lookup the key and get the range */
	emptyreply = justcount ? shared.czero : shared.emptymultibulk;
	if ((o = lookupKeyReadOrReply(c,c->argv[1],emptyreply)) == NULL ||
		checkType(c,o,REDIS_TS)) return;

	mp  = o->ptr;
	zsl = mp->zsl;

	/* If reversed, assume the elements are sorted from high to low time. */
	ln = zslFirstWithScore(zsl,range.min);

	/* No "first" element in the specified interval. */
	if (ln == NULL) {
		addReply(c,emptyreply);
		return;
	}

	/* We don't know in advance how many matching elements there
	 * are in the list, so we push this object that will represent
	 * the multi-bulk length in the output buffer, and will "fix"
	 * it later */
	if (!justcount)
		replylen = addDeferredMultiBulkLength(c);


	while (ln) {
		/* Check if this this element is in range. */
		if (range.maxex) {
			/* Element should have time < range.max */
			if (ln->score >= range.max) break;
		} else {
			/* Element should have time <= range.max */
			if (ln->score > range.max) break;
		}

		/* Do our magic */
		rangelen++;
		if (!justcount) {
			if (withtimes)
				addReplyDouble(c,ln->score);
			if (withvalues) {
				value = ln->obj;
				incrRefCount(value);
				addReplyBulk(c,value);
			}
		}

		ln = ln->level[0].forward;
	}

	if (justcount) {
		addReplyLongLong(c,(long)rangelen);
	} else {
		setDeferredMultiBulkLength(c, replylen, rangelen*(withtimes+withvalues));
	}
}


/*
void theadCommand(redisClient *c) {
    robj *o;
    zskiplistNode *ln;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptymultibulk)) == NULL
            || checkType(c,o,REDIS_TS)) return;
    zset *mp   = o->ptr;
    ln = mp->zsl->header->level[0].forward;
    addReplyBulk(c,ln->obj);
}


void ttailCommand(redisClient *c) {
    robj *o;
    zskiplistNode *ln;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptymultibulk)) == NULL
            || checkType(c,o,REDIS_TS)) return;
    zset *mp   = o->ptr;
    ln = mp->zsl->tail;
    addReplyBulk(c,ln->obj);
}
*/
