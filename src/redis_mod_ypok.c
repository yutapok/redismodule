#include <redismodule.h>
#include "redis_ypok.h"
#define MODULE_NAME "ypok"
#define MODULE_VERSION 1


int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
	if(RedisModule_Init(ctx, MODULE_NAME, MODULE_VERSION, REDISMODULE_APIVER_1)==REDISMODULE_ERR){
		return REDISMODULE_ERR;
	}

	//zstd_set
	//IN) zstd_set <key> <string value>
	//OUT) OK
	//
	if(RedisModule_CreateCommand(ctx, MODULE_NAME ".zstd_set", ZstdSetCommand, "write", 1, 1, 1) == REDISMODULE_ERR){
		return  REDISMODULE_ERR;
	}


	//zstd_get
	//IN) zstd_get <key>
	//OUT) <val>
	//
	if(RedisModule_CreateCommand(ctx, MODULE_NAME ".zstd_get", ZstdGetCommand, "readonly", 1, 1, 1) == REDISMODULE_ERR){
		return  REDISMODULE_ERR;
	}

	return REDISMODULE_OK;
}
