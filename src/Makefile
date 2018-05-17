CC = gcc
CSRC = redis_mod_ypok.c redis_ypok.c
OBJS = redis_mod_ypok.o redis_ypok.o
OPPFLAGS += -fPIC -Wall -O2  -I. -I/usr/local/include/redis -c
SOPPFLAGS += -shared -Wl,-z,relro -Wl,-soname,rmod_ypok.so
LIBS = -lzstd
PROGRAM = redis_mod_ypok
SOPROGRAM = rmod_ypok
MODTGT = redis_mod_ypok
CTGT = redis_ypok

all: $(PROGRAM) $(SOPROGRAM)
$(PROGRAM): $(CSRC)
	$(CC) $(OPPFLAGS) -o $(MODTGT).o $(MODTGT).c $(LIBS)
	$(CC) $(OPPFLAGS) -o $(CTGT).o $(CTGT).c $(LIBS)

$(SOPROGRAM): $(OBJS)
	$(CC) $(SOPPFLAGS) $(OBJS)  -o $(SOPROGRAM).so $(LIBS)


clean:;
	rm -f *.*o
