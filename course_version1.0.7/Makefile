CC		= g++
FLAGS	= -O2 -D_FILE_OFFSET_BITS=64

.PHONY : all clean

all :
	$(MAKE) --directory=client
	$(MAKE) --directory=lib
	$(CC) $(FLAGS) -L/usr/local/BerkeleyDB.5.2/lib -o run main.cpp client/*.o lib/*.o -ldb_cxx

clean :
	$(MAKE) --directory=client clean
	$(MAKE) --directory=lib clean
	rm -f run data/*


