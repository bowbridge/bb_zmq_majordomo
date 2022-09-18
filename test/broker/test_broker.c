//
// Created by joerg on 9/5/22.
//

#include "../include/test_broker.h"
#include <stdio.h>
#include <czmq.h>

#include "../../include/mdp_broker.h"
#include "../../include/mdp_worker_msg.h"

#define WORKER_PK "CZxDrWQr75SAjw3WNYfan4Vnn-cePBolSIYIyf47N0M"
#define WORKER_SK "_q-xRN6-BcK-zRvd1HgxG7ytMB0n6jnSBwry3H38-6Q"

#define BROKER_PK "ZT77-JRva8XUh5-1po6iCTyNeNNFkJXJhCz6ztIirUw"
#define BROKER_SK "to2PhEjc4_Os3BaW6sspMm2Wcz2z7qQJ84seDPxi4J4"
char *broker_bind_endpoint = "ipc:///tmp/mdp.ipc";
//char *broker_bind_endpoint = "tcp://localhost:9002";
//char *broker_bind_endpoint = "tcp://*:9002";

int main() {

    zactor_t *broker = zactor_new(mdp_broker, "server");
    // zstr_send(broker, "VERBOSE");
    zstr_sendx(broker, "KEYS", BROKER_PK, BROKER_SK, "/home/joerg/authkeys.txt", NULL);
    zstr_sendx(broker, "BIND", broker_bind_endpoint, NULL);

    fprintf(stdout, "************************ Broker is running. Press a key to stop it\r\n");
    getchar();

   
    printf("************************ Destroying broker\r\n");
    zactor_destroy(&broker);
    sleep(1);
    fprintf(stdout, "************************ Exiting\r\n");
}
