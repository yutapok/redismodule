CC = gcc
OBJS = redis_mod_ypok.o redis_ypok.o
OPPFLAGS += -fPIC -Wall -O2  -I. -c
SOPPFLAGS += -shared -Wl,-z,relro -Wl,-soname,rmod_ypok.so
LIBS = -lzstd
PROGRAM = redis_module_ypok
SOPROGRAM = rmod_ypok
MODTGT = redis_mod_ypok
CTGT = redis_ypok

all: $(PROGRAM) $(SOPROGRAM)
$(PROGRAM): $(MODTGT).c  $(CTGT).c
	$(CC) $(OPPFLAGS) -o $(MODTGT).o $(MODTGT).c $(LIBS)
	$(CC) $(OPPFLAGS) -o $(CTGT).o $(CTGT).c $(LIBS)

$(SOPROGRAM): $(OBJS)
	$(CC) $(SOPPFLAGS) $(OBJS)  -o $(SOPROGRAM).so $(LIBS)


clean:;
	rm -f *.*o
