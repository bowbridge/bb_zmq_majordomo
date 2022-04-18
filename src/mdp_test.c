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

int
main() {
    printf("hello mdp_test\n");


    //char *endpoint = "tcp://localhost:9002";
    //char *endpoint_bind = "tcp://*:9002";
    char *endpoint = "ipc:///tmp/mdp.ipc";
    char *endpoint_bind = endpoint;
    //mdp_client_t *client = mdp_client_new(endpoint, (unsigned char *) BROKER_PK);
    mdp_client_t *client = mdp_client_new(endpoint, NULL);
    mdp_client_set_verbose(client);

    zactor_t *broker = zactor_new(mdp_broker, "server");
    zstr_send(broker, "VERBOSE");
    zstr_sendx(broker, "KEYS", BROKER_PK, BROKER_SK, "/home/joerg/authkeys.txt", NULL);
    zstr_sendx(broker, "BIND", endpoint_bind, NULL);


    sleep(1);

    char *service = "MAKE COFFEE";
    mdp_worker_t *worker = mdp_worker_new(endpoint, service, (unsigned char *) WORKER_PK, (unsigned char *) BROKER_PK);
    //  mdp_worker_t *worker2 = mdp_worker_new(endpoint, service, (unsigned char *) WORKER_PK, (unsigned char *) BROKER_PK);
    //  mdp_worker_t *worker3 = mdp_worker_new(endpoint, service, (unsigned char *) WORKER_PK, (unsigned char *) BROKER_PK);
    assert(worker);
    //  assert(worker2);
    //   assert(worker3);
    mdp_worker_set_verbose(worker);
    sleep(5);

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

    if (0) {

        zmsg_t *msg = zmsg_new();
        assert(msg);
        int res = zmsg_addstrf(msg, "This is a super-secret message");
        assert(res == 0);


        mdp_client_request(client, service, &msg);
        msg = zmsg_recv(worker);
        zsock_t *worker_sock = mdp_worker_msgpipe(worker);
        char *cmd = NULL;


        zframe_t *address;
        zmsg_t *message;
        res = zsock_recv(worker_sock, "sfm", &cmd,
                         &address, &message);
        printf("res= %d. \n", res);
        printf("Got command: %s\n", cmd);



        // Process the message.
        zframe_t *first = zmsg_first(message);
        char *first_str = zframe_strdup(first);
        printf("Got first message: %s\n", first_str);


        char response[64];
        sprintf(response, "Partial response to %s", first_str);
        zmsg_t *msg_response = zmsg_new();
        zmsg_addstr(msg_response, response);

        // Make a copy of address, because mdp_worker_send_partial will destroy it.
        zframe_t *address2 = zframe_dup(address);
        mdp_worker_send_partial(worker, &address2, &msg_response);

        // Wait for partial reponse.
        zsock_t *client_sock = mdp_client_msgpipe(client);
        res = zsock_recv(client_sock, "sm", &cmd, &message);
        printf("Client (2): got command %s\n", cmd);
        printf(" Response body:\n");
        zmsg_print(message);
        zmsg_destroy(&message);

        sprintf(response, "Final response to %s", first_str);
        msg_response = zmsg_new();
        zmsg_addstr(msg_response, response);

        mdp_worker_send_final(worker, &address, &msg_response);

        // Wait for final response.
        res = zsock_recv(client_sock, "sm", &cmd, &message);
        printf("Client (2): got command %s\n", cmd);
        printf(" Response body:\n");
        zmsg_print(message);

    }
    //printf("Press Enter to stop the worker");
    //getchar();
    sleep(10);
    printf("************************ Destroying broker\r\n");
    zactor_destroy(&broker);
    sleep(30);
    printf("************************ restarting broker\r\n");
    broker = zactor_new(mdp_broker, "server");
    zstr_send(broker, "VERBOSE");
    zstr_sendx(broker, "KEYS", BROKER_PK, BROKER_SK, "/home/joerg/authkeys.txt", NULL);
    zstr_sendx(broker, "BIND", endpoint_bind, NULL);
    sleep(10);

    mdp_worker_destroy(&worker);
    //   mdp_worker_destroy(&worker2);
    //   mdp_worker_destroy(&worker3);
    sleep(20);

    mdp_client_destroy(&client);

    sleep(20);


    return 0;
}
