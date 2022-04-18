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
#define HEARTBEAT_DELAY 2500

static int s_worker_encrypt_body(zmsg_t *body, unsigned char *key) {
    if (NULL != body && NULL != key) {
        // encrypt the original body - frame by frame
        int num_frames = (int) zmsg_size(body);

        // prepend identifier pubkey and frames
        zmsg_addstr(body, "BB_MDP_SECURE");
        unsigned char initial_nonce[crypto_secretbox_NONCEBYTES];
        randombytes_buf(initial_nonce, crypto_secretbox_NONCEBYTES);
        zmsg_addmem(body, initial_nonce, crypto_secretbox_NONCEBYTES);
        unsigned char nonce[crypto_secretbox_NONCEBYTES];
        memcpy(nonce, initial_nonce, crypto_secretbox_NONCEBYTES);

        int i = 0;
        // zsys_debug("WORKER: Encrypting with key %2x %2x ... %2x %2x ", key[0], key[1],key[crypto_kx_SESSIONKEYBYTES - 2],key[crypto_kx_SESSIONKEYBYTES - 1]);
        for (i = 0; i < num_frames; i++) {
            zframe_t *frame = zmsg_pop(body);
            if (NULL != frame) {

                size_t data_plain_len = zframe_size(frame);
                unsigned char *data_plain = (unsigned char *) zframe_data(frame);
                size_t data_encrypted_len = crypto_secretbox_MACBYTES + data_plain_len;
                unsigned char *data_encrypted = (unsigned char *) zmalloc(data_encrypted_len);
                if (NULL == data_encrypted) {
                    zsys_error("Memory allocation error");
                    return -1;
                }
                int res = crypto_secretbox_easy(data_encrypted, data_plain,
                                                data_plain_len,
                                                nonce, key);
                // zsys_debug("WORKER: Encrypting frame %d - %s", i + 1, res == 0 ? "SUCESS" : "ERROR");
                if (res != 0) {
                    return -1;
                }

                zmsg_addmem(body, data_encrypted, data_encrypted_len);
                free(data_encrypted);
                zframe_destroy(&frame);
                // increment the nonce for the next frame (if any)
                sodium_increment(nonce, crypto_secretbox_NONCEBYTES);
            }
        }
        // add the "canary" frame
        char *canary = "BB_MDP_SECURE";
        unsigned char *data_encrypted = (unsigned char *) zmalloc(strlen(canary) + crypto_secretbox_MACBYTES);
        if (0 == crypto_secretbox_easy(data_encrypted, (unsigned char *) canary,
                                       strlen(canary),
                                       nonce, key)) {
            zmsg_addmem(body, data_encrypted, strlen(canary) + crypto_secretbox_MACBYTES);
        } else {
            if (data_encrypted) {
                free(data_encrypted);
            }
            return -1;
        }
        free(data_encrypted);
        return 0;
    }
    return -1;
}

static int s_worker_decrypt_body(zmsg_t *body, unsigned char *key) {
    if (NULL != body && NULL != key) {
        // decrypt the body, frame by frame
        unsigned char nonce[crypto_secretbox_NONCEBYTES];
        zframe_t *frame = zmsg_pop(body);
        if (frame) {
            // nonce frame
            memcpy(nonce, zframe_data(frame), crypto_secretbox_NONCEBYTES);
            zframe_destroy(&frame);
            int num_frames = (int) zmsg_size(body) - 1;
            int i = 0;
            // zsys_debug("WORKER: Decrypting with key %2x %2x ... %2x %2x ", key[0], key[1],key[crypto_kx_SESSIONKEYBYTES - 2],key[crypto_kx_SESSIONKEYBYTES - 1]);
            for (i = 0; i < num_frames; i++) {

                frame = zmsg_pop(body);
                if (frame) {
                    unsigned char *ciphertext = zframe_data(frame);
                    if (ciphertext) {
                        size_t ciphertextlen = zframe_size(frame);
                        size_t plaintextlen = ciphertextlen - crypto_secretbox_MACBYTES;
                        unsigned char *plaintext = (unsigned char *) zmalloc(plaintextlen);
                        if (plaintext) {
                            int res = crypto_secretbox_open_easy(plaintext, ciphertext,
                                                                 (unsigned long long int) ciphertextlen, nonce,
                                                                 key);
                            if (0 != res) {
                                zsys_error("WORKER: Failed to decrypt data frame #%d", (i + 1));
                                return -1;
                            }
                            zmsg_addmem(body, plaintext, plaintextlen);
                            zframe_destroy(&frame);
                            // zsys_debug("WORKER: decrypted data frame #%d", (i + 1));
                            // increment the nonce for the next frame (if any)
                            sodium_increment(nonce, crypto_secretbox_NONCEBYTES);
                        }
                    }
                }
            }
            // get/decrypt "Canary" frame
            frame = zmsg_pop(body);
            size_t ciphertextlen = zframe_size(frame);
            size_t plaintextlen = ciphertextlen - crypto_secretbox_MACBYTES;
            unsigned char *plaintext = (unsigned char *) zmalloc(plaintextlen);
            int res = crypto_secretbox_open_easy(plaintext, zframe_data(frame),
                                                 (unsigned long long int) ciphertextlen, nonce,
                                                 key);
            if (0 != res ||
                0 != memcmp(plaintext, "BB_MDP_SECURE", strlen("BB_MDP_SECURE"))) {
                zsys_error("WORKER: Decryption error - Check failed");
                zmsg_destroy(&body);
                zframe_destroy(&frame);
                return -1;
            }
        }
        return 0;
    }
    return -1;
}


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
    if (verbose) {
        printf("\n");
    }
    //  @selftest
    zactor_t *client = zactor_new(mdp_worker, NULL);
    if (verbose) {
        zstr_send(client, "VERBOSE");
    }
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
        //  zsys_debug("connected to %s", self->args->endpoint);
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

    // Check if this is encrypted
    zmsg_t *body = mdp_worker_msg_body(worker_msg);
    zframe_t *frame = zmsg_pop(body);
    if (frame) {
        if (zframe_streq(frame, "BB_MDP_SECURE")) {
            s_worker_decrypt_body(body, self->session_key_rx);
        }
    }

    zsock_send(self->msgpipe, "sfm", "REQUEST",
               mdp_worker_msg_address(worker_msg),
               body);
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
    if (self->session_key_tx) {
        free(self->session_key_tx);
        self->session_key_tx = NULL;
    }

    if (self->session_key_rx) {
        free(self->session_key_rx);
        self->session_key_rx = NULL;
    }

    if (self->auth_key) {
        free(self->auth_key);
        self->auth_key = NULL;
    }

    if (self->broker_pk) {
        free(self->broker_pk);
        self->broker_pk = NULL;
    }
}


//  ---------------------------------------------------------------------------
//  prepare_ready_message
//

static void
prepare_ready_message(client_t *self) {
    self->service = strdup(self->args->service); // TODO: is this needed?
    mdp_worker_msg_set_service(self->message, self->service);
    if (NULL != self->auth_key && NULL != self->broker_pk) {
        // zsys_debug("mdp_worker:            $ preparing key exchange");

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
    // Encrypt the body if we need to
    if (NULL != self->session_key_tx) {
        s_worker_encrypt_body(self->args->reply_body, self->session_key_tx);
    } else {
        zmsg_pushstr(self->args->reply_body, "BB_MDP_PLAIN");
    }
    mdp_worker_msg_set_body(msg, &self->args->reply_body);
}


//  ---------------------------------------------------------------------------
//  prepare_final_response
//

static void
prepare_final_response(client_t *self) {
    mdp_worker_msg_t *msg = self->message;
    mdp_worker_msg_set_address(msg, &self->args->address);
    // Encrypt the body if we need to
    if (NULL != self->session_key_tx) {
        s_worker_encrypt_body(self->args->reply_body, self->session_key_tx);
    } else {
        zmsg_pushstr(self->args->reply_body, "BB_MDP_PLAIN");
    }
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

static int
check_timeouts(client_t *self) {
    self->timeouts++;
    if (self->timeouts == MAX_TIMEOUTS) {
        // engine_set_exception(self, destructor_event);
        // Set exception to constructor event to the worker reconnexts
        engine_set_exception(self, constructor_event);
        return -1;
    }
    return 0;
}
