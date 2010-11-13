#include "redis.h"

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
 * The api looks like the hash api, implementation is almost equivalent to
 * the zset container. */


/*
 * Added 2 new objects:
 *
 * #define REDIS_MAP 5				-->		map
 * #define REDIS_SCORE_VALUE 6 		-->		mapValue
 */

/*
 * Commands:
 *
 * tlen		-->		size of map
 * tadd		-->		add items
 * texists	-->		check if key is in map
 * tget		-->		get value at key
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


/* Get the value from a hash identified by key. Returns either a string
 * object or NULL if the value cannot be found. The refcount of the object
 * is always increased by 1 when the value was found. */
robj *mapTypeGet(robj *o, robj *key) {
    robj *value = NULL;
    map *mp = o->ptr;
	dictEntry *de = dictFind(mp->dict,key);
	if (de != NULL) {
		value = ((mapValue*)((robj*)dictGetEntryVal(de))->ptr)->value;
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
