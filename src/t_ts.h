/*
 * t_ts.h
 *
 *  First created on: 14 Nov 2010
 *      Author: lsbardel
 */

#ifndef _TIMESERIES_H
#define _TIMESERIES_H

#include "redis.h"

#define REDIS_TS 5

/*
 * 7 TIMESERIES COMMANDS
 */
void tslenCommand(redisClient *c);
void tsexistsCommand(redisClient *c);
void tsaddCommand(redisClient *c);
void tsgetCommand(redisClient *c);
void tsrangeCommand(redisClient *c);
void tsrangebytimeCommand(redisClient *c);
void tscountCommand(redisClient *c);


/* ZSET internals commands - Implemented in t_zset.c */

/* Struct to hold a inclusive/exclusive range spec. */
typedef struct {
    double min, max;
    int minex, maxex; /* are min or max exclusive? */
} zrangespec;

zskiplistNode *zslFirstWithScore(zskiplist *zsl, double score);
zskiplistNode* zslistTypeGetElementByRank(zskiplist *zsl, unsigned long rank);
int zslParseRange(robj *min, robj *max, zrangespec *spec);


/* Timeseries internals */
robj *createTsObject(void);
int tsTypeSet(robj *o, double time, robj *key, robj *value);
int tsTypeExists(robj *o, robj *key);
robj *tsTypeGet(robj *o, robj *key);
robj *tsTypeLookupWriteOrCreate(redisClient *c, robj *key);
void tsrangeGenericCommand(redisClient *c, int start, int end, int withtimes, int withvalues, int reverse);
void tsrangebytimeGenericCommand(redisClient *c, int reverse, int justcount);
void tsrangeRemaining(redisClient *c, int *withtimes, int *withvalues);


#endif /* _TIMESERIES_H */
