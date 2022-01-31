#include <stdio.h>
#include <czmq.h>

#include "../include/mdp_client.h"
#include "../include/mdp_worker.h"
#include "../include/mdp_broker.h"
#include "../include/mdp_worker_msg.h"



int
main()
{
    int res;



    printf("hello mdp_test\n");
    int verbose = 1;
    char *endpoint = "tcp://localhost:6002";
    char *endpoint_bind = "tcp://*:6002";
    char *service = "MAKE COFFEE";
    const char*                  public_key     = "0dqj+9FV}@-?PxfIAr@-)B)-@j2ATo]s5D0q%FTm";
    const char*                  secret_key     = "e3.:dPEAO(2T4#nQ!CaFt$aJwN#1>853JeGs:=8q";
    uint8_t sec[32];
    uint8_t pub[32];
    zmq_z85_decode(sec, secret_key);
    zmq_z85_decode(pub, public_key);
    zcert_t * cert = zcert_new_from(pub, sec);
    int plain=0;



    // Set up the broker
    zactor_t *broker = zactor_new(mdp_broker, "broker");
    if (verbose){
        zstr_send(broker, "VERBOSE");
    }
    if(plain == 0)
        zstr_sendx(broker, "SETUP-CURVE", public_key, secret_key, CURVE_ALLOW_ANY, NULL);
 
    zstr_sendx(broker, "BIND", endpoint_bind, NULL);


    // set up the client
    char * client_identity="CLIENT1234";
    mdp_client_t *client = plain==1?mdp_client_new(endpoint, NULL,  NULL, NULL):mdp_client_new(endpoint, client_identity, cert, public_key);    
    assert(client);
    if (verbose)
    { 
      mdp_client_set_verbose(client);
    }
    
    

    // Set up the worker
    char * worker_identity = "WORKER2345";
    mdp_worker_t *worker = plain==1?mdp_worker_new(endpoint, service, NULL, NULL, NULL):mdp_worker_new(endpoint, service, worker_identity, cert, public_key);
    assert(worker);
    if (verbose)
    { 
        mdp_worker_set_verbose(worker);      
    }


    zmsg_t *msg = zmsg_new();
    assert(msg);
    res = zmsg_addstr(msg, "Message");
    assert(res == 0);

    res = mdp_client_request(client, service, &msg);
//    msg = zmsg_recv(worker);
    zsock_t *worker_sock = mdp_worker_msgpipe(worker);
    char *cmd = zstr_recv(worker_sock);
    printf("Got command: %s\n", cmd);
    
    zframe_t *address;
    zmsg_t *message;
    res = zsock_recv(worker_sock, "fm",
        &address, &message);
    
    // Process the message.
    zframe_t *first = zmsg_first(message);
    char *first_str = zframe_strdup(first);
    printf("Got message: %s\n", first_str);
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


    printf("Press Enter to stop");
    getchar();

    
    mdp_worker_destroy(&worker);
    mdp_client_destroy(&client);
    zactor_destroy(&broker);
    return 0;
}
