#! /bin/bash
gcc -o broker test_broker.c  -lczmq -lzmq -L ../../build/src/.libs/ -lmajordomo -lsodium
