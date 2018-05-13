#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <redismodule.h>
#include "redis_ypok.h"
#include <zstd.h>
#define COMP_LEVEL 1

int _RM_SetString(RedisModuleCtx *ctx, RedisModuleString *key, RedisModuleString *val);
int _CompressSet(RedisModuleCtx *ctx, RedisModuleString *key, RedisModuleString *val);

int ZstdSetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
	if (argc < 2){
		return RedisModule_WrongArity(ctx);
	}

	int rc = _CompressSet(ctx, argv[1], argv[2]);
	RedisModule_ReplicateVerbatim(ctx);
	RedisModule_ReplyWithSimpleString(ctx,"OK");
	return rc;
}

int _CompressSet(RedisModuleCtx *ctx, RedisModuleString *key, RedisModuleString *val)
{
	int rc;
	RedisModuleString *zstd_val = NULL;
	size_t size_of_data;
	char *raw_data = (char *)RedisModule_StringPtrLen(val, &size_of_data);
        size_t bound = ZSTD_compressBound(size_of_data);
        void* compressed = RedisModule_Alloc(bound);	
	size_t actual = ZSTD_compress(compressed, bound ,raw_data, size_of_data, COMP_LEVEL);
	zstd_val =  RedisModule_CreateString(ctx, compressed, actual);
	rc = _RM_SetString(ctx, key, zstd_val);
	return rc;

}

int _RM_SetString(RedisModuleCtx *ctx, RedisModuleString *key, RedisModuleString *val)
{
	int rc = REDISMODULE_OK;
	RedisModuleString *tx_key = NULL;
	tx_key = RedisModule_OpenKey(ctx, key, REDISMODULE_READ|REDISMODULE_WRITE);
	RedisModule_StringSet(tx_key, val);
	
	RedisModule_CloseKey(tx_key);
	
	return rc;
}
