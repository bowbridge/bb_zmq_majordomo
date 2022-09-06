#! /bin/bash
gcc -o worker testworker.c -lpthread -lczmq -lzmq -L ../../build/src/.libs/ -lmajordomo -lsodium
