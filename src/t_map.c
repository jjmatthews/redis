#include "redis.h"

#include <math.h>

/*-----------------------------------------------------------------------------
 * Map API
 *----------------------------------------------------------------------------*/

/* A map is Redis implementation of sorted associative container which
 * uses two data structures to hold keys, values and scores in order to obtain
 * O(log(N)) INSERT and REMOVE operations and O(1) RETRIEVAL.
 *
 * Values are ordered with respect a score, same as zsets, but can be accessed
 * using keys, same as hashes.
 * The values are added to an hash table mapping Redis objects to keys.
 * At the same time the keys are added to a skip list to maintain
 * sorting with respect a score.
 *
 * The api looks like the hash api, implementation is almost equivalent to
 * the zset container. */

/*-----------------------------------------------------------------------------
 * Map commands
 *----------------------------------------------------------------------------*/

void mlenCommand(redisClient *c) {
    robj *o;
    map *zs;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_MAP)) return;

    zs = o->ptr;
    addReplyLongLong(c,zs->zsl->length);
}

void maddCommand(redisClient *c) {
	int i;
	double scoreval;
	robj *o;

	if ((c->argc % 3) == 1) {
		addReplyError(c,"wrong number of arguments for MADD");
		return;
	}

	/*
	zsetobj = lookupKeyWrite(c->db,key);
	    if (zsetobj == NULL) {
	        zsetobj = createZsetObject();
	        dbAdd(c->db,key,zsetobj);
	    } else {
	        if (zsetobj->type != REDIS_ZSET) {
	            addReply(c,shared.wrongtypeerr);
	            return;
	        }
	    }
	    zs = zsetobj->ptr;
	    */
	if ((o = mapTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
	hashTypeTryConversion(o,c->argv,2,c->argc-1);
	for (i = 2; i < c->argc; i += 3) {
		if(getDoubleFromObjectOrReply(c,c->argv[i],&scoreval,NULL) != REDIS_OK) return;
		hashTypeTryObjectEncoding(o,&c->argv[i+1], &c->argv[i+2]);
	    mapTypeSet(o,scoreval,c->argv[i],c->argv[i+1]);
	}
	addReply(c, shared.ok);
	touchWatchedKey(c->db,c->argv[1]);
	server.dirty++;
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


/*-----------------------------------------------------------------------------
 * Sorted set commands
 *----------------------------------------------------------------------------*/

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
