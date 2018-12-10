#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <redismodule.h>
#include "redis_ypok.h"
#include <zstd.h>
#define COMP_LEVEL 1

int _CompressSet(RedisModuleCtx *ctx, RedisModuleString *key, RedisModuleString *val);
int _CompressGet(RedisModuleCtx *ctx, RedisModuleString *key);

int ZstdSetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (argc < 2){
        return RedisModule_WrongArity(ctx);
    }

    int rc = REDISMODULE_OK;
    if( _CompressSet(ctx, argv[1], argv[2]) == REDISMODULE_ERR){
        rc = REDISMODULE_OK;
        RedisModule_ReplyWithNull(ctx);
        goto finally;
    }
    RedisModule_ReplyWithSimpleString(ctx,"OK");

finally:
    return rc;
}



int ZstdGetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (argc < 1){
        return RedisModule_WrongArity(ctx);
    }
    size_t actual;
    int rc = REDISMODULE_OK;
    if(_CompressGet(ctx, argv[1]) == REDISMODULE_ERR){
        rc = REDISMODULE_ERR;
        RedisModule_ReplyWithNull(ctx);
        goto finally;
    }

finally:
    return rc;
}




int _CompressSet(RedisModuleCtx *ctx, RedisModuleString *key, RedisModuleString *val)
{
    int rc = REDISMODULE_OK;
    RedisModuleString *zstd_val = NULL;
    size_t size_of_data;
    char *raw_data = (char *)RedisModule_StringPtrLen(val, &size_of_data);
    size_t bound = ZSTD_compressBound(size_of_data);
    void* compressed = RedisModule_Alloc(bound + 1);
    
    size_t actual = ZSTD_compress(compressed, bound ,raw_data, size_of_data, COMP_LEVEL);
    if(ZSTD_isError(actual)){
        rc = REDISMODULE_ERR;
        goto finally;
    }


    zstd_val =  RedisModule_CreateString(ctx, compressed, actual);

    RedisModuleString *tx_key = NULL;
    tx_key = RedisModule_OpenKey(ctx, key, REDISMODULE_READ|REDISMODULE_WRITE);
    RedisModule_StringSet(tx_key, zstd_val);
    RedisModule_CloseKey(tx_key);

finally:
    if (compressed != NULL){
        RedisModule_Free(compressed); compressed=NULL;
    }
    return rc;
}


int _CompressGet(RedisModuleCtx *ctx, RedisModuleString *key)
{
    int rc = REDISMODULE_OK;
    size_t len_compress;
    RedisModuleKey *tx_key = NULL;
    char *compressed = NULL;

    tx_key = RedisModule_OpenKey(ctx, key, REDISMODULE_READ|REDISMODULE_WRITE);
    compressed = RedisModule_StringDMA(tx_key, &len_compress, REDISMODULE_READ|REDISMODULE_WRITE);
    RedisModule_CloseKey(tx_key);

    if (len_compress == 0){
        rc = REDISMODULE_ERR;
        goto finally;
    }


    unsigned long long len_decompress = ZSTD_getDecompressedSize(compressed, len_compress);
    void* decompress = RedisModule_Alloc((size_t)len_decompress + 1);

    size_t d_size = ZSTD_decompress(decompress, len_decompress, compressed, len_compress);
    if(ZSTD_isError(d_size)){
        rc = REDISMODULE_ERR;
        goto finally;
    }

    RedisModule_ReplyWithStringBuffer(ctx, (char *)decompress, d_size);


finally:
    if( compressed != NULL ){
        RedisModule_Free(decompress);compressed=NULL;
    }

  return rc;
}


