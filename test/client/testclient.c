#include <stdio.h>
#include <czmq.h>
#include "../../include/mdp_client.h"


#define WORKER_PK "CZxDrWQr75SAjw3WNYfan4Vnn-cePBolSIYIyf47N0M"
#define WORKER_SK "_q-xRN6-BcK-zRvd1HgxG7ytMB0n6jnSBwry3H38-6Q"

#define BROKER_PK "ZT77-JRva8XUh5-1po6iCTyNeNNFkJXJhCz6ztIirUw"
#define BROKER_SK "to2PhEjc4_Os3BaW6sspMm2Wcz2z7qQJ84seDPxi4J4"
char *endpoint = "ipc:///tmp/mdp.ipc";
#define CLIENTS 100
#define REQUESTS 10
int exterminate = 0;

void *client_function(void *p) {
    mdp_client_t *client = mdp_client_new(endpoint, (unsigned char *) BROKER_PK);
    char *service = "MAKE COFFEE";
    int i = 0;
    for (i = 0; i < REQUESTS; i++) {
        char *cmd = NULL;

        zmsg_t *client_request = zmsg_new();
        assert(client_request);
        int res = zmsg_addstrf(client_request, "This is a super-secret message");
        assert(res == 0);
        mdp_client_request(client, service, &client_request);
        zsock_t *client_sock = mdp_client_msgpipe(client);

        // Wait for partial reponse.
        zmsg_t *client_partial_reply = NULL;

        res = zsock_recv(client_sock, "sm", &cmd, &client_partial_reply);
        printf("Client (2): got command %s\n", cmd);
        free(cmd);
        printf(" Response body:\n");
        zmsg_print(client_partial_reply);
        zmsg_destroy(&client_partial_reply);


        // Wait for final response.
        zmsg_t *client_final_reply;
        res = zsock_recv(client_sock, "sm", &cmd, &client_final_reply);
        printf("Client (2): got command %s\n", cmd);
        free(cmd);
        printf(" Response body:\n");
        zmsg_print(client_final_reply);
        zmsg_destroy(&client_final_reply);

    }
}


int main() {

    pthread_t client_thread[CLIENTS];
    int i = 0;
    for (i = 0; i < CLIENTS; i++) {
        fprintf(stderr, "*** Creating worker thread # %d\r\n", i);
        pthread_create(&client_thread[i], 0, client_function, (void *) NULL);
    }

    fprintf(stderr, "*** %d workers created - press a key to stop\r\n", i);

    exterminate = 1;
    for (i = 0; i < CLIENTS; i++) {
        fprintf(stderr, "*** joining worker thread # %d\r\n", i);
        pthread_join(client_thread[i], NULL);
    }
    exit(0);

}