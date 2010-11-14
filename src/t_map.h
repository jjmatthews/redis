/*
 * t_map.h
 *
 *  Created on: 14 Nov 2010
 *      Author: lsbardel
 */

#ifndef __T_MAP_H_
#define __T_MAP_H_

#include "redis.h"


/*
 * 11 MAP COMMANDS
 */
void tlenCommand(redisClient *c);
void texistsCommand(redisClient *c);
void taddCommand(redisClient *c);
void tgetCommand(redisClient *c);
void theadCommand(redisClient *c);
void ttailCommand(redisClient *c);
void tkeysCommand(redisClient *c);
void titemsCommand(redisClient *c);
void trangeCommand(redisClient *c);
void trangebyscoreCommand(redisClient *c);
void tcountCommand(redisClient *c);



/* Map internals */
int mapTypeSet(robj *o, double score, robj *key, robj *value);
int mapTypeExists(robj *o, robj *key);
robj *mapTypeGet(robj *o, robj *key);
robj *mapTypeLookupWriteOrCreate(redisClient *c, robj *key);
void trangeGenericCommand(redisClient *c, int start, int end, int withscores, int withvalues, int reverse);
void trangebyscoreGenericCommand(redisClient *c, int reverse, int justcount);
void trangeRemaining(redisClient *c, int *withscores, int *withvalues);


#endif /* __T_MAP_H_ */
