#include "t_map.h"

#include <math.h>

/*-----------------------------------------------------------------------------
 * Map API
 *----------------------------------------------------------------------------*/

/* A map is Redis implementation of a unique sorted associative container which
 * uses two data structures to hold keys, values and scores in order to obtain
 * O(log(N)) on INSERT and REMOVE operations and O(1) on RETRIEVAL via keys.
 *
 * Values are ordered with respect to scores (double values) same as zsets,
 * but are accessed using keys, same as hashes.
 * The values are added to an hash table mapping Redis objects to keys.
 * At the same time the keys are added to a skip list to maintain
 * sorting with respect scores.
 *
 * The api looks very much like the hash api, the implementation on the other end
 * is almost equivalent to the zset container. */

/*
 * Added 2 new objects:
 *
 * #define REDIS_MAP 5				-->		map
 * #define REDIS_SCORE_VALUE 6 		-->		mapValue
 */

/*
 * Commands:
 *
 * tlen				-->		size of map
 * tadd				-->		add items as multiple of triplet (score,key,value)
 * texists			-->		check if key is in map
 * tget				-->		get value at key
 * thead			-->		head key
 * ttail			-->		tail key
 * tkeys			-->		ordered keys (from skiplist)
 * titems			-->		ordered keys,value pair
 * trange			-->		range by rank in skiplist
 * trangebyscore	-->		range by score in skiplist
 * tcount			-->		count element in range (by score obviously)
 */

/*-----------------------------------------------------------------------------
 * Map commands
 *----------------------------------------------------------------------------*/

void tlenCommand(redisClient *c) {
    robj *o;
    map *mp;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_MAP)) return;

    mp = o->ptr;
    addReplyLongLong(c,mp->zsl->length);
}


void texistsCommand(redisClient *c) {
    robj *o;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_MAP)) return;

    addReply(c, mapTypeExists(o,c->argv[2]) ? shared.cone : shared.czero);
}


void tgetCommand(redisClient *c) {
	robj *o, *value;
	if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk)) == NULL ||
		checkType(c,o,REDIS_MAP)) return;

	if ((value = mapTypeGet(o,c->argv[2])) != NULL) {
		addReplyBulk(c,value);
		decrRefCount(value);
	} else {
		addReply(c,shared.nullbulk);
	}
}


void taddCommand(redisClient *c) {
	int i;
	double scoreval;
	robj *o;

	if ((c->argc % 3) == 1) {
		addReplyError(c,"wrong number of arguments for TADD");
		return;
	}

	if ((o = mapTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
	hashTypeTryConversion(o,c->argv,2,c->argc-1);
	for (i = 2; i < c->argc; i += 3) {
		if(getDoubleFromObjectOrReply(c,c->argv[i],&scoreval,NULL) != REDIS_OK) return;
		hashTypeTryObjectEncoding(o,&c->argv[i+1], &c->argv[i+2]);
	    mapTypeSet(o,scoreval,c->argv[i+1],c->argv[i+2]);
	}
	addReply(c, shared.ok);
	touchWatchedKey(c->db,c->argv[1]);
	server.dirty++;
}


void theadCommand(redisClient *c) {
	robj *o;
	zskiplistNode *ln;

	if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptymultibulk)) == NULL
			|| checkType(c,o,REDIS_MAP)) return;
	map *mp   = o->ptr;
	ln = mp->zsl->header->level[0].forward;
	addReplyBulk(c,ln->obj);
}


void ttailCommand(redisClient *c) {
	robj *o;
	zskiplistNode *ln;

	if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptymultibulk)) == NULL
			|| checkType(c,o,REDIS_MAP)) return;
	map *mp   = o->ptr;
	ln = mp->zsl->tail;
	addReplyBulk(c,ln->obj);
}


void tkeysCommand(redisClient *c) {
	trangeGenericCommand(c,0,-1,0,0,0);
}


void titemsCommand(redisClient *c) {
	trangeGenericCommand(c,0,-1,0,1,0);
}


void trangeCommand(redisClient *c) {
	long start = 0;
	long end = 0;
	int withvalues = 0;
	int withscores = 0;
	if ((getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != REDIS_OK) ||
		(getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != REDIS_OK)) return;
	trangeRemaining(c,&withscores,&withvalues);
	trangeGenericCommand(c,start,end,withscores,withvalues,0);
}


void trangebyscoreCommand(redisClient *c) {
	trangebyscoreGenericCommand(c,0,0);
}

void tcountCommand(redisClient *c) {
	trangebyscoreGenericCommand(c,0,1);
}


/*-----------------------------------------------------------------------------
 * Internals
 *----------------------------------------------------------------------------*/


robj *mapTypeLookupWriteOrCreate(redisClient *c, robj *key) {
    robj *o = lookupKeyWrite(c->db,key);
    if (o == NULL) {
        o = createMapObject();
        dbAdd(c->db,key,o);
    } else {
        if (o->type != REDIS_MAP) {
            addReply(c,shared.wrongtypeerr);
            return NULL;
        }
    }
    return o;
}


/* Test if the key exists in the given map. Returns 1 if the key
 * exists and 0 when it doesn't. */
int mapTypeExists(robj *o, robj *key) {
	map *mp = o->ptr;
    if (dictFind(mp->dict,key) != NULL) {
    	return 1;
    }
    return 0;
}


mapValue* toMapType(void* o) {
	return ((mapValue*)((robj*)o)->ptr);
}

/* Get the value from a hash identified by key. Returns either a string
 * object or NULL if the value cannot be found. The refcount of the object
 * is always increased by 1 when the value was found. */
robj *mapTypeGet(robj *o, robj *key) {
    robj *value = NULL;
    map *mp = o->ptr;
	dictEntry *de = dictFind(mp->dict,key);
	if (de != NULL) {
		value = toMapType(dictGetEntryVal(de))->value;
		incrRefCount(value);
	}
    return value;
}

/* Add an element, discard the old if the key already exists.
 * Return 0 on insert and 1 on update. */
int mapTypeSet(robj *o, double score, robj *key, robj *value) {
	int update = 0;
    robj *ro;
    dictEntry *de;
    map *mp;
    zskiplistNode *znode;
    mp = o->ptr;

    /* We need to remove and re-insert the element when it was already present
     * in the dictionary, to update the skiplist. Note that we delay adding a
     * pointer to the score because we want to reference the score in the
     * skiplist node. */
    if (dictAdd(mp->dict,key,NULL) == DICT_OK) {
    	/* New element */
        incrRefCount(key); /* added to hash */
        znode = zslInsert(mp->zsl,score,key);
        incrRefCount(key); /* added to skiplist */

        /* Update the score in the dict entry */
        de = dictFind(mp->dict,key);
        redisAssert(de != NULL);
        dictGetEntryVal(de) = createMapValue(score,value);
        update = 1;
    } else {
    	mapValue *curobj;

        /* Update score */
        de = dictFind(mp->dict,key);
        redisAssert(de != NULL);
        ro = dictGetEntryVal(de);
        curobj = ro->ptr;
        decrRefCount(curobj->value);
        curobj->value = value;
        incrRefCount(value);

        /* When the score is updated, reuse the existing string object to
         * prevent extra alloc/dealloc of strings on ZINCRBY. */
        if (score != curobj->score) {
            redisAssert(zslDelete(mp->zsl,curobj->score,key));
            znode = zslInsert(mp->zsl,score,key);
            curobj->score = score;
            update = 1;
        }
    }
    return update;
}


void trangeRemaining(redisClient *c, int *withscores, int *withvalues) {
	/* Parse optional extra arguments. Note that ZCOUNT will exactly have
		 * 4 arguments, so we'll never enter the following code path. */
	if (c->argc > 4) {
		int remaining = c->argc - 4;
		int pos = 4;

		while (remaining) {
			if (remaining >= 1 && !strcasecmp(c->argv[pos]->ptr,"withscores")) {
				pos++; remaining--;
				*withscores = 1;
			} else if (remaining >= 1 && !strcasecmp(c->argv[pos]->ptr,"withvalues")) {
				pos++; remaining--;
				*withvalues = 1;
			} else {
				addReply(c,shared.syntaxerr);
				return;
			}
		}
	}
}


void trangeGenericCommand(redisClient *c, int start, int end, int withscores, int withvalues, int reverse) {
    robj *o;
    int llen;
    int rangelen, j;
    map *mp;
    zskiplist *zsl;
    zskiplistNode *ln;
    robj *key;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptymultibulk)) == NULL
             || checkType(c,o,REDIS_MAP)) return;
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
    addReplyMultiBulkLen(c,rangelen*(1+withscores+withvalues));
    for (j = 0; j < rangelen; j++) {
        key = ln->obj;
        addReplyBulk(c,key);
        if (withvalues)
        	addReplyBulk(c,mapTypeGet(o,key));
        if (withscores)
            addReplyDouble(c,ln->score);
        ln = reverse ? ln->backward : ln->level[0].forward;
    }
}



void trangebyscoreGenericCommand(redisClient *c, int reverse, int justcount) {
	/* No reverse implementation for now */
	zrangespec range;
	map *mp;
	zskiplist *zsl;
	zskiplistNode *ln;
	robj *o, *key, *emptyreply;
	int withvalues = 0;
	int withscores = 0;
	unsigned long rangelen = 0;
	void *replylen = NULL;

	/* Parse the range arguments. */
	if (zslParseRange(c->argv[2],c->argv[3],&range) != REDIS_OK) {
		addReplyError(c,"min or max is not a double");
		return;
	}
	trangeRemaining(c,&withvalues,&withvalues);

	/* Ok, lookup the key and get the range */
	emptyreply = justcount ? shared.czero : shared.emptymultibulk;
	if ((o = lookupKeyReadOrReply(c,c->argv[1],emptyreply)) == NULL ||
		checkType(c,o,REDIS_MAP)) return;

	mp  = o->ptr;
	zsl = mp->zsl;

	/* If reversed, assume the elements are sorted from high to low score. */
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
			/* Element should have score < range.max */
			if (ln->score >= range.max) break;
		} else {
			/* Element should have score <= range.max */
			if (ln->score > range.max) break;
		}

		/* Do our magic */
		rangelen++;
		if (!justcount) {
			key = ln->obj;
			addReplyBulk(c,key);
			if (withvalues)
				addReplyBulk(c,mapTypeGet(o,key));
			if (withscores)
				addReplyDouble(c,ln->score);
		}

		ln = ln->level[0].forward;
	}

	if (justcount) {
		addReplyLongLong(c,(long)rangelen);
	} else {
		setDeferredMultiBulkLength(c, replylen, rangelen*(1+withscores+withvalues));
	}
}
