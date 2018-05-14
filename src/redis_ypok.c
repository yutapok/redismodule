#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <redismodule.h>
#include "redis_ypok.h"
#include <zstd.h>
#define COMP_LEVEL 1

int _RM_SetString(RedisModuleCtx *ctx, RedisModuleString *key, RedisModuleString *val);
char *_RM_GetString(RedisModuleCtx *ctx, RedisModuleString *key, size_t *len_compress);
int _CompressSet(RedisModuleCtx *ctx, RedisModuleString *key, RedisModuleString *val);
int ZstdGetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int* _CompressGet(RedisModuleCtx *ctx, RedisModuleString *key);

int ZstdSetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
	if (argc < 2){
		return RedisModule_WrongArity(ctx);
	}

	int rc = _CompressSet(ctx, argv[1], argv[2]);
	RedisModule_ReplicateVerbatim(ctx);
	return rc;
}

int ZstdGetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
  if (argc < 1){
    return RedisModule_WrongArity(ctx);
  }
  size_t actual;
  int rc = _CompressGet(ctx, argv[1]);
  return REDISMODULE_OK;
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
	RedisModule_ReplyWithSimpleString(ctx,"OK");
	return rc;

}

int *_CompressGet(RedisModuleCtx *ctx, RedisModuleString *key)
{
  size_t len_compress;

  char *compressed = _RM_GetString(ctx, key, &len_compress);
  RedisModuleString *tx_key = NULL;
  unsigned long long len_decompress = ZSTD_getDecompressedSize(compressed, len_compress);
  void* decompress = RedisModule_Alloc((size_t)len_decompress);
  size_t d_size = ZSTD_decompress(decompress, len_decompress, compressed, len_compress);
  RedisModule_ReplyWithStringBuffer(ctx, (char *)decompress, d_size);
  RedisModule_Free(decompress);compressed=NULL;
  return REDISMODULE_OK;
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

char *_RM_GetString(RedisModuleCtx *ctx, RedisModuleString *key, size_t *len_compress)
{
    RedisModuleString *tx_key = NULL;
    tx_key = RedisModule_OpenKey(ctx, key, REDISMODULE_READ|REDISMODULE_WRITE);
    size_t len;
    char *compressed_buf = RedisModule_StringDMA(tx_key, &len, REDISMODULE_READ|REDISMODULE_WRITE);
    *len_compress = len;
    RedisModule_CloseKey(tx_key);
    return compressed_buf;
}
