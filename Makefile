all:
	gcc -o run prihash.c sechash.c original_main.c BF_64.a -static -w
custom:
	gcc -o run prihash.c sechash.c our_main.c BF_64.a -static -w