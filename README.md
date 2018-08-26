# redismodule  
## test
simple rediscommands with compression or decompression

* make  
```
    cd src
    make
```

* prepare
```
    vim /etc/redis.conf
    loadmodule ${PATH}/rmod_ypok.so
```

* usage redis-cli
```
    ypok.zstd_get <key>
    ypok.zstd_set <key> <val>
    ypok.zstd_hget <key> <field>
    ypok.zstd_hset <key> <field> <value>
```

## prepared  
https://github.com/facebook/zstd/blob/dev/examples/simple_compression.c  
https://github.com/antirez/redis  

## refer  
https://redis-module-redoc.readthedocs.io/en/latest/  
https://progur.com/2016/09/how-to-install-and-use-zstd-facebook.html  

