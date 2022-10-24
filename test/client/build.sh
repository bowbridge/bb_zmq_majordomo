#! /bin/bash
gcc -o client testclient.c -lpthread -lczmq -lzmq -L ../../build/src/.libs/ -lmajordomo -lsodium
