#include <zstd.h>

#include <redismodule.h>
#include "redis_ypok.h"
#define COMP_LEVEL 19

int _RM_SetString(RedisModuleCtx, *ctx, RedisModuleString *key, RedisModuleString *val);
int _CompressSet(RedisModuleCtx, *ctx, RedisModuleString *key, RedisModuleString *val);

int ZstdSetCommand(RedisModuleCtx, *ctx, RedisModuleString **argv, int argc)
{
	if (argc < 2){
		retrun RedisModule_WrongArity(ctx);
	}
	
	int rc = _CompressSet(ctx, argv[1], argv[2]);
	RedisModule_ReplicateVerbatim(ctx);
	RedisModule_ReplyWithString(ctx, REDISMODULE_OK);
	return rc;
}

int _CompressSet(RedisModuleCtx, *ctx, RedisModuleString *key, RedisModuleString *val)
{
	RedisModuleString *zstd_val = NULL;
	size_t size_of_data;
	char *raw_data = (const char*)RedisModule_StringPtrLen(val, &size_of_data);
        size_t estimated_size_of_compressed_data = ZSTD_compressBound(size_of_data);
        void* compressed_data = malloc(estimated_size_of_compressed_data);
	
	size_t actual_size_of_compressed_data =
        ZSTD_compress(compressed_data, estimated_size_of_compressed_data,
            raw_data, size_of_data, COMP_LEVEL);
	zstd_val =  RedisModule_CreateString(ctx, (char*)compressed_data, actual_size_of_compressed_data)
	rc = _RM_SetString(ctx, key, zstd_val);
	return rc;

}

int _RM_SetString(RedisModuleCtx, *ctx, RedisModuleString *key, RedisModuleString *val)
{
	RedisModuleString *key = NULL;
	tx_key = RedisModule_OpenKey(ctx, key, REDISMODULE_READ|REDISMODULE_WRITE);
	rc = RedisModule_SetString(tx_key, val);
	
	RedisModule_CloseKey(tx_key);
	
	return rc;
}
