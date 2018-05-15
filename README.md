# redismodule

##prepare
https://github.com/facebook/zstd/blob/dev/examples/simple_compression.c \n
https://github.com/antirez/redis \n

##usage(This will be transformed to Makefile)
gcc -fPIC -Wall -O2  -I. -I/usr/local/include/redis  -c -o redis_mod_ypok.o redis_mod_ypok.c
gcc -fPIC -Wall -O2  -I. -I/usr/local/include/redis  -c -o redis_ypok.o redis_ypok.c -lzstd \n
gcc -shared -Wl,-z,relro -Wl,-soname,rmod_ypok.so redis_mod_ypok.o redis_ypok.o  -o rmod_ypok.so -lzstd \n

##refer
https://redis-module-redoc.readthedocs.io/en/latest/ \n
https://progur.com/2016/09/how-to-install-and-use-zstd-facebook.html \n

