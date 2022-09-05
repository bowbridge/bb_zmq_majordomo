//
// Created by joerg on 9/5/22.
//

#include <stdio.h>
#include <czmq.h>
#include "testworker.h"
#include "../../include/mdp_worker.h"

#define WORKER_PK "CZxDrWQr75SAjw3WNYfan4Vnn-cePBolSIYIyf47N0M"
#define WORKER_SK "_q-xRN6-BcK-zRvd1HgxG7ytMB0n6jnSBwry3H38-6Q"
#define NUM_WORKERS 50

char *endpoint = "ipc:///tmp/mdp.ipc";
#define BROKER_PK "ZT77-JRva8XUh5-1po6iCTyNeNNFkJXJhCz6ztIirUw"
int exterminate = 0;

void *worker_function(void *v_service) {
    char *service = (char *) v_service;
    mdp_worker_t *worker = mdp_worker_new(endpoint, service, (unsigned char *) WORKER_PK, (unsigned char *) BROKER_PK);
    if (worker) {
        zsock_t *worker_sock = mdp_worker_msgpipe(worker);
        if (worker_sock) {
            char *cmd = NULL;
            zframe_t *address;
            zmsg_t *worker_request;
            zsock_set_rcvtimeo(worker_sock, 200); // timeout 200ms
            while (!exterminate) {
                int res = zsock_recv(worker_sock, "sfm", &cmd,
                                     &address, &worker_request);
                if (0 == res) {
                    printf("Got command: %s\n", cmd);
                    free(cmd);
                    // Process the message.
                    zframe_t *first = zmsg_first(worker_request);
                    char *first_str = zframe_strdup(first);
                    printf("Got first message: %s\n", first_str);
                    zmsg_destroy(&worker_request);

                    char response[64];

                    sprintf(response, "Partial response to %s", first_str);
                    zmsg_t *worker_response = zmsg_new();
                    zmsg_addstr(worker_response, response);
                    zframe_t *address_partial_reply = zframe_dup(address); // duplicate
                    mdp_worker_send_partial(worker, &address_partial_reply, &worker_response);
                    zclock_sleep(200);
                    sprintf(response, "Final response to %s", first_str);
                    free(first_str);
                    zmsg_t *worker_final_response = zmsg_new();
                    zmsg_addstr(worker_final_response, response);
                    mdp_worker_send_final(worker, &address, &worker_final_response);
                }
            }
        } else {
            fprintf(stderr, "Worker socket creation failed\r\n");
        }
        mdp_worker_destroy(&worker);
    } else {
        fprintf(stderr, "Worker creation failed\r\n");
    }
    return NULL;
}

int main() {

    pthread_t worker_thread[NUM_WORKERS];
    int i = 0;
    for (i = 0; i < NUM_WORKERS; i++) {
        fprintf(stderr, "*** Creating worker thread # %d\r\n", i);
        pthread_create(&worker_thread[i], 0, worker_function, (void *) "MAKE COFFEE");
    }

    fprintf(stderr, "*** %d workers created - press a key to stop\r\n", i);

    getchar();
    exterminate = 1;
    for (i = 0; i < NUM_WORKERS; i++) {
        fprintf(stderr, "*** joining worker thread # %d\r\n", i);
        pthread_join(worker_thread[i], NULL);
    };
    exit(0);

}