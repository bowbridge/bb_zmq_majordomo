/*  =========================================================================
    mdp_worker - Majordomo Worker

    Copyright (c) the Contributors as noted in the AUTHORS file.
    This file is part of majordomo, a C implementation of Majordomo
    Protocol:
    https://github.com/ajanicij/majordomo.git
                                                                       
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
    =========================================================================
*/

/*
@header
    Description of class for man page.
@discuss
    Detailed discussion of the class, if any.
@end
*/

//  TODO: Change these to match your project's needs
#include "../include/mdp_worker_msg.h"
#include "../include/mdp_worker.h"
#include "../include/mdp_msg.h"

//  Forward reference to method arguments structure
typedef struct _client_args_t client_args_t;

//  This structure defines the context for a client connection
typedef struct {
    //  These properties must always be present in the client_t
    //  and are set by the generated engine. The cmdpipe gets
    //  messages sent to the actor; the msgpipe may be used for
    //  faster asynchronous message flows.
    zsock_t *cmdpipe;           //  Command pipe to/from caller API
    zsock_t *msgpipe;           //  Message pipe to/from caller API
    zsock_t *dealer;            //  Socket to talk to server
    mdp_worker_msg_t *message;  //  Message to/from server
    client_args_t *args;        //  Arguments from methods

    //  TODO: Add specific properties for your application
    char *service;
    unsigned int timeouts;      // Number of timeouts
    unsigned char *auth_key;
    unsigned char *broker_pk;
    unsigned char *session_key_rx;
    unsigned char *session_key_tx;
} client_t;

//  Include the generated client engine
#include "mdp_worker_engine.inc"

// Maximum number of timeouts; If this number is reached, we stop sending
// heartbeats and terminate connection.
#define MAX_TIMEOUTS 3

// Interval for sending heartbeat [ms]
#define HEARTBEAT_DELAY 1000

//  Allocate properties and structures for a new client instance.
//  Return 0 if OK, -1 if failed

static int
client_initialize(client_t *self) {
    self->service = NULL; // Service will be set via constructor.
    self->timeouts = 0;
    return 0;
}

//  Free properties and structures for a client instance

static void
client_terminate(client_t *self) {
    //  Destroy properties here
    free(self->service);
}


//  ---------------------------------------------------------------------------
//  Selftest

void
mdp_worker_test(bool verbose) {
    printf(" * mdp_worker: ");
    if (verbose)
        printf("\n");

    //  @selftest
    zactor_t *client = zactor_new(mdp_worker, NULL);
    if (verbose)
        zstr_send(client, "VERBOSE");
    zactor_destroy(&client);
    //  @end
    printf("OK\n");
}


//  ---------------------------------------------------------------------------
//  connect_to_server
//

static void
connect_to_server(client_t *self) {
    if (zsock_connect(self->dealer, "%s", self->args->endpoint)) {
        engine_set_exception(self, connect_error_event);
        zsys_warning("could not connect to %s", self->args->endpoint);
        zsock_send(self->cmdpipe, "si", "FAILURE", 0);
    } else {
        zsys_debug("connected to %s", self->args->endpoint);
        zsock_send(self->cmdpipe, "si", "SUCCESS", 0);
    }
}


//  ---------------------------------------------------------------------------
//  handle_connect_error
//

static void
handle_connect_error(client_t *self) {
    engine_set_next_event(self, destructor_event);
}



//  ---------------------------------------------------------------------------
//  signal_connection_success
//

static void
signal_connection_success(client_t *self) {

}


//  ---------------------------------------------------------------------------
//  signal_request
//

static void
signal_request(client_t *self) {
    mdp_worker_msg_t *worker_msg = self->message;
    zsock_send(self->msgpipe, "sfm", "REQUEST",
               mdp_worker_msg_address(worker_msg),
               mdp_worker_msg_body(worker_msg));
}


//  ---------------------------------------------------------------------------
//  log_protocol_error
//

static void
log_protocol_error(client_t *self) {

}


//  ---------------------------------------------------------------------------
//  received_heartbeat
//

static void
received_heartbeat(client_t *self) {
}


//  ---------------------------------------------------------------------------
//  destroy_worker
//

static void
destroy_worker(client_t *self) {
}


//  ---------------------------------------------------------------------------
//  prepare_ready_message
//

static void
prepare_ready_message(client_t *self) {
    self->service = strdup(self->args->service); // TODO: is this needed?
    mdp_worker_msg_set_service(self->message, self->service);
    if (NULL != self->auth_key && NULL != self->broker_pk) {
        zsys_debug("mdp_worker:            $ preparing key exchange");

        // ephemeral keypair
        unsigned char client_pk[crypto_kx_PUBLICKEYBYTES], client_sk[crypto_kx_SECRETKEYBYTES];
        /* Generate the key pair */
        crypto_kx_keypair(client_pk, client_sk);
        /*  Compute two shared keys using the server's public key and the client's secret key.
            client_rx will be used by the client to receive data from the server,
            client_tx will by used by the client to send data to the server. */

        // base64 decode the broker PK
        unsigned char broker_pk[crypto_kx_PUBLICKEYBYTES];
        size_t binlen = 0;
        sodium_base642bin(broker_pk, crypto_kx_PUBLICKEYBYTES, (const char *) self->broker_pk,
                          strlen((char *) self->broker_pk),
                          NULL, &binlen, NULL, sodium_base64_VARIANT_URLSAFE_NO_PADDING);

        // allocate session key buffers
        self->session_key_rx = (unsigned char *) zmalloc(crypto_kx_SESSIONKEYBYTES);
        self->session_key_tx = (unsigned char *) zmalloc(crypto_kx_SESSIONKEYBYTES);

        // generate keys;
        if (crypto_kx_client_session_keys(self->session_key_rx, self->session_key_tx,
                                          client_pk, client_sk, broker_pk) == 0) {
            // attach keys to READY message
            zmsg_t *body = zmsg_new();
            zmsg_addstr(body, (char *) self->auth_key);
            zmsg_addmem(body, client_pk, crypto_kx_PUBLICKEYBYTES);
            mdp_worker_msg_set_ready_body(self->message, &body);
        }
    }
}


//  ---------------------------------------------------------------------------
//  prepare_partial_response
//

static void
prepare_partial_response(client_t *self) {
    mdp_worker_msg_t *msg = self->message;
    mdp_worker_msg_set_address(msg, &self->args->address);
    mdp_worker_msg_set_body(msg, &self->args->reply_body);
}


//  ---------------------------------------------------------------------------
//  prepare_final_response
//

static void
prepare_final_response(client_t *self) {
    mdp_worker_msg_t *msg = self->message;
    mdp_worker_msg_set_address(msg, &self->args->address);
    mdp_worker_msg_set_body(msg, &self->args->reply_body);
}


//  ---------------------------------------------------------------------------
//  handle_set_wakeup
//

static void
handle_set_wakeup(client_t *self) {
    engine_set_wakeup_event(self, HEARTBEAT_DELAY, send_heartbeat_event);
}


//  ---------------------------------------------------------------------------
//  reset_timeouts
//

static void
reset_timeouts(client_t *self) {
    self->timeouts = 0;
}


//  ---------------------------------------------------------------------------
//  handle_set_verbose
//

static void
handle_set_verbose(client_t *self) {
    mdp_worker_verbose = true;
}


//  ---------------------------------------------------------------------------
//  check_timeouts
//

static void
check_timeouts(client_t *self) {
    self->timeouts++;
    if (self->timeouts == MAX_TIMEOUTS) {
        engine_set_exception(self, destructor_event);
    }
}
