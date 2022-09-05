#include <stdio.h>
#include <czmq.h>
#include "../include/mdp_client.h"
#include "../include/mdp_worker.h"
#include "../include/mdp_broker.h"
#include "../include/mdp_worker_msg.h"

#define WORKER_PK "CZxDrWQr75SAjw3WNYfan4Vnn-cePBolSIYIyf47N0M"
#define WORKER_SK "_q-xRN6-BcK-zRvd1HgxG7ytMB0n6jnSBwry3H38-6Q"

#define BROKER_PK "ZT77-JRva8XUh5-1po6iCTyNeNNFkJXJhCz6ztIirUw"
#define BROKER_SK "to2PhEjc4_Os3BaW6sspMm2Wcz2z7qQJ84seDPxi4J4"
char *endpoint = "ipc:///tmp/mdp.ipc";
#define NUM_WORKERS 100

int
main() {
    printf("hello mdp_test\n");


    //char *endpoint = "tcp://localhost:9002";
    //char *endpoint_bind = "tcp://*:9002";

    char *endpoint_bind = endpoint;
    //mdp_client_t *client = mdp_client_new(endpoint);
    mdp_client_t *client = mdp_client_new(endpoint, (unsigned char *) BROKER_PK);
    //mdp_client_t *client = mdp_client_new(endpoint, NULL);
    //mdp_client_set_verbose(client);

    zactor_t *broker = zactor_new(mdp_broker, "server");
    // zstr_send(broker, "VERBOSE");
    zstr_sendx(broker, "KEYS", BROKER_PK, BROKER_SK, "/home/joerg/authkeys.txt", NULL);
    zstr_sendx(broker, "BIND", endpoint_bind, NULL);


    sleep(1);

    char *service = "MAKE COFFEE";
    mdp_worker_t *workers[NUM_WORKERS];
    int i = 0;
    for (i = 0; i < NUM_WORKERS; i++) {
        //workers[i] = mdp_worker_new(endpoint, service);
        //workers[i] = mdp_worker_new(endpoint, service, NULL, NULL);
        workers[i] = mdp_worker_new(endpoint, service, (unsigned char *) WORKER_PK, (unsigned char *) BROKER_PK);
        //mdp_worker_set_verbose(workers[i]);
    }
    //  mdp_worker_t *worker2 = mdp_worker_new(endpoint, service, (unsigned char *) WORKER_PK, (unsigned char *) BROKER_PK);
    //  mdp_worker_t *worker3 = mdp_worker_new(endpoint, service, (unsigned char *) WORKER_PK, (unsigned char *) BROKER_PK);
    //assert(worker);
    //  assert(worker2);
    //   assert(worker3);
    //mdp_worker_set_verbose(worker);
    sleep(1);

//    printf("** trying mmi\r\n");
//    zmsg_t *mmi_msg = zmsg_new();
//    zmsg_addstr(mmi_msg, service);
//    mdp_client_request(client, "mmi.service", &mmi_msg);
//    zsock_t *client_sock = mdp_client_msgpipe(client);
//    char *_cmd = NULL;
//    zmsg_t *_message = NULL;
//    zsock_recv(client_sock, "sm", &_cmd, &_message);
//    printf("Client got reply %s\n", _cmd);
//    printf(" Response body:\n");
//    free(_cmd);
//    _cmd = NULL;
//    zmsg_print(_message);
//    zmsg_destroy(&_message);
//
//    mmi_msg = zmsg_new();
//    zmsg_addstr(mmi_msg, service);
//    mdp_client_request(client, "mmi.workers", &mmi_msg);
//    zsock_recv(client_sock, "sm", &_cmd, &_message);
//    printf("Client got reply %s\n", _cmd);
//    printf(" Response body:\n");
//    zmsg_print(_message);
//    zmsg_destroy(&_message);

    i = 0;
    for (i = 0; i < 1; i++) {

        zmsg_t *client_request = zmsg_new();
        assert(client_request);
        int res = zmsg_addstrf(client_request, "This is a super-secret message");
        assert(res == 0);
        mdp_client_request(client, service, &client_request);
        zsock_t *client_sock = mdp_client_msgpipe(client);


        zsock_t *worker_sock = mdp_worker_msgpipe(workers[0]);
        char *cmd = NULL;
        zframe_t *address;
        zmsg_t *worker_request;
        res = zsock_recv(worker_sock, "sfm", &cmd,
                         &address, &worker_request);

        printf("res= %d. \n", res);
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
        mdp_worker_send_partial(workers[0], &address_partial_reply, &worker_response);


        // Wait for partial reponse.
        zmsg_t *client_partial_reply = NULL;

        res = zsock_recv(client_sock, "sm", &cmd, &client_partial_reply);
        printf("Client (2): got command %s\n", cmd);
        free(cmd);
        printf(" Response body:\n");
        zmsg_print(client_partial_reply);
        zmsg_destroy(&client_partial_reply);


        sprintf(response, "Final response to %s", first_str);
        free(first_str);
        zmsg_t *worker_final_response = zmsg_new();
        zmsg_addstr(worker_final_response, response);
        mdp_worker_send_final(workers[0], &address, &worker_final_response);

        // Wait for final response.
        zmsg_t *client_final_reply;
        res = zsock_recv(client_sock, "sm", &cmd, &client_final_reply);
        printf("Client (2): got command %s\n", cmd);
        free(cmd);
        printf(" Response body:\n");
        zmsg_print(client_final_reply);
        zmsg_destroy(&client_final_reply);

    }



    /* for (i = 0; i < 5; i++) {
         char *result = NULL;
         char *_cmd = NULL;
         zsock_t *client_sock = mdp_client_msgpipe(client);
         zmsg_t *mmi_msg = NULL;
         mmi_msg = zmsg_new();
         zmsg_addstr(mmi_msg, service);
         mdp_client_request(client, "mmi.workers", &mmi_msg);
         zsock_recv(client_sock, "ss", &_cmd, &result);
         free(_cmd);
         zmsg_destroy(&mmi_msg);
         char *waiting = NULL;
         mmi_msg = zmsg_new();
         zmsg_addstr(mmi_msg, service);
         mdp_client_request(client, "mmi.waiting", &mmi_msg);
         zsock_recv(client_sock, "ss", &_cmd, &waiting);
         zsys_debug("*************************************  Workers: %s, Waiting: %s", result, waiting);
         free(_cmd);
         free(waiting);

         if (result)
             free(result);

         sleep(1);
     }
     */
    /*   mdp_worker_destroy(&workers[0]);
       mdp_worker_destroy(&workers[1]);
       mdp_worker_destroy(&workers[2]);
       mdp_worker_destroy(&workers[3]);
       for (i = 0; i < 20; i++) {
           zmsg_t *mmi_msg = zmsg_new();
           zsock_t *client_sock = mdp_client_msgpipe(client);
           char *_cmd = NULL;
           char *result = NULL;

           mmi_msg = zmsg_new();
           zmsg_addstr(mmi_msg, service);
           mdp_client_request(client, "mmi.workers", &mmi_msg);
           zsock_recv(client_sock, "ss", &_cmd, &result);
           free(_cmd);
           char *waiting = NULL;
           mmi_msg = zmsg_new();
           zmsg_addstr(mmi_msg, service);
           mdp_client_request(client, "mmi.waiting", &mmi_msg);
           zsock_recv(client_sock, "ss", &_cmd, &waiting);
           zsys_debug("*************************************  Workers: %s, Waiting: %s", result, waiting);
           free(_cmd);
           free(result);
           free(waiting);
           sleep(1);
       }
       workers[0] = mdp_worker_new(endpoint, service, (unsigned char *) WORKER_PK,
                                   (unsigned char *) BROKER_PK);
       workers[1] = mdp_worker_new(endpoint, service, (unsigned char *) WORKER_PK,
                                   (unsigned char *) BROKER_PK);
       workers[2] = mdp_worker_new(endpoint, service, (unsigned char *) WORKER_PK,
                                   (unsigned char *) BROKER_PK);
       workers[3] = mdp_worker_new(endpoint, service, (unsigned char *) WORKER_PK,
                                   (unsigned char *) BROKER_PK);
                                         */

    zmsg_t *mmi_msg;

/*

    printf("************************ Destroying broker\r\n");
    zactor_destroy(&broker);
    sleep(30);
    printf("************************ restarting broker\r\n");
    broker = zactor_new(mdp_broker, "server");
    //zstr_send(broker, "VERBOSE");
    zstr_sendx(broker, "KEYS", BROKER_PK, BROKER_SK, "/home/joerg/authkeys.txt", NULL);
    zstr_sendx(broker, "BIND", endpoint_bind, NULL);
*/
    for (i = 0; i < 50; i++) {
        mmi_msg = zmsg_new();
        zsock_t *client_sock = mdp_client_msgpipe(client);
        char *_cmd = NULL;
        char *result = NULL;

        mmi_msg = zmsg_new();
        zmsg_addstr(mmi_msg, "json");
        mdp_client_request(client, "mmi.status", &mmi_msg);
        zsock_recv(client_sock, "ssm", &_cmd, &result, &mmi_msg);
        zsys_debug(result);
        free(_cmd);
        sleep(1);
    }


    for (i = 0; i < NUM_WORKERS; i++) {
        zsys_debug("Destroying worker %p", workers[i]);
        mdp_worker_destroy(&workers[i]);
    }

    for (i = 0; i < 40; i++) {
        mmi_msg = zmsg_new();
        zsock_t *client_sock = mdp_client_msgpipe(client);
        char *_cmd = NULL;
        char *result = NULL;

        mmi_msg = zmsg_new();
        zmsg_addstr(mmi_msg, service);
        mdp_client_request(client, "mmi.status", &mmi_msg);
        zsock_recv(client_sock, "ssm", &_cmd, &result, &mmi_msg);
        zsys_debug(result);
        free(_cmd);
        zmsg_destroy(&mmi_msg);
        sleep(1);
    }


    getchar();
    mdp_client_destroy(&client);

    //sleep(20);
    zactor_destroy(&broker);


    return 0;
}
