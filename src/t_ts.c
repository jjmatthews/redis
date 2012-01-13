#include "t_ts.h"

#include <math.h>


/*-----------------------------------------------------------------------------
 * timeseries commands
 *----------------------------------------------------------------------------*/

void tslenCommand(redisClient *c) {
    robj *o;
    zskiplist *ts;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_TS)) return;

    ts = o->ptr;
    addReplyLongLong(c,ts->length);
}


void tsexistsCommand(redisClient *c) {
    robj *o;
    double score;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_TS)) return;

    if(getDoubleFromObjectOrReply(c,c->argv[2],&score,NULL) != REDIS_OK) return;

    addReply(c, tsTypeExists(o,score) ? shared.cone : shared.czero);
}

void tsrankCommand(redisClient *c) {
    robj *o;
    double score;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_TS)) return;

    if(getDoubleFromObjectOrReply(c,c->argv[2],&score,NULL) != REDIS_OK) return;

    addReply(c, tsTypeRank(o,score) ? shared.cone : shared.czero);
}


void tsgetCommand(redisClient *c) {
	robj *o, *value;
	double score;

	if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk)) == NULL ||
		checkType(c,o,REDIS_TS)) return;

    if(getDoubleFromObjectOrReply(c,c->argv[2],&score,NULL) != REDIS_OK)  {
        addReplyError(c,"Could not convert score to double.");
        return;
    }

	if ((value = tsTypeGet(o,score)) != NULL) {
		addReplyBulk(c,value);
		decrRefCount(value);
	} else {
		addReply(c,shared.nullbulk);
	}
}


void tsaddCommand(redisClient *c) {
	int i,update;
	double score;
	robj *o;

	if ((c->argc % 2) == 1) {
		addReplyError(c,"wrong number of arguments for TADD");
		return;
	}

	if ((o = tsTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
	hashTypeTryConversion(o,c->argv,2,c->argc-1);
	update = 0;
	for (i = 2; i < c->argc; i += 2) {
		if(getDoubleFromObjectOrReply(c,c->argv[i],&score,NULL) != REDIS_OK) return;
	    update += tsTypeSet(o,score,c->argv[i+1]);
	}
	addReplyLongLong(c,update);
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

/* Find the first node having a score equal or greater than the specified one.
* Returns NULL if there is no match. */
zskiplistNode *zslFirstWithScore(zskiplist *zsl, double score) {
    zskiplistNode *x;
    int i;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward && x->level[i].forward->score < score)
            x = x->level[i].forward;
    }
    /* We may have multiple elements with the same score, what we need
* is to find the element with both the right score and object. */
    return x->level[0].forward;
}


robj *createTsObject(void) {
    zskiplist *zs = zslCreate();
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
int tsTypeExists(robj *o, double time) {
	zskiplist *ts = o->ptr;
	zskiplistNode* ln = zslFirstWithScore(ts,time);
	return ln != NULL && ln->score == time ? 1 : 0;
}


/* Get the rank of a given score(time) */
unsigned long tsTypeRank(robj *o, double score) {
    zskiplist *ts = o->ptr;
    zskiplistNode *x = ts->header;
    unsigned long rank = 0;
    int i;

    for (i = ts->level-1; i >= 0; i--) {
        while (x->level[i].forward && x->level[i].forward->score < score) {
            rank += x->level[i].span;
            x = x->level[i].forward;
        }
    }
    if(x->level[0].forward->score == score) {
        rank += x->level[0].span;
        return rank;
    } else
        return NULL;
}


/* Get the value at time. */
robj *tsTypeGet(robj *o, double score) {
    robj *value = NULL;
    zskiplist *ts = o->ptr;
    zskiplistNode* ln = zslFirstWithScore(ts,score);
	if (ln != NULL && ln->score == score) {
		value = ln->obj;
		incrRefCount(value);
	}
    return value;
}

/* Add an element, discard the old if the key already exists.
 * Return 0 on insert and 1 on update. */
int tsTypeSet(robj *o, double score, robj *value) {
	int update = 0;
    robj *ro;
    zskiplist *ts;
    zskiplistNode *ln;
    ts = o->ptr;

    ln = zslFirstWithScore(ts, score);
    if(ln != NULL && ln->score == score) {
    	//Update member in skiplist
    	ro = ln->obj;
    	decrRefCount(ro);
    	ln->obj = value;
    	incrRefCount(value);
    } else {
    	/* New element */
        ln = zslInsert(ts,score,value);
        incrRefCount(value);
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
    zskiplist *ts;
    zskiplistNode *ln;
    robj *value;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptymultibulk)) == NULL
             || checkType(c,o,REDIS_TS)) return;
	ts   = o->ptr;
	llen = ts->length;

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
        ln = start == 0 ? ts->tail :
                            zslGetElementByRank(ts, llen-start);
    } else {
        ln = start == 0 ? ts->header->level[0].forward :
                            zslGetElementByRank(ts, start+1);
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
	zskiplist *ts;
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

	ts  = o->ptr;

	/* If reversed, assume the elements are sorted from high to low time. */
	ln = zslFirstWithScore(ts,range.min);

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
