/*  =========================================================================
    mdp_client - Majordomo Client

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
#include "../include/mdp_client_msg.h"
#include "../include/mdp_client.h"

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
    mdp_client_msg_t *message;  //  Message to/from server
    client_args_t *args;        //  Arguments from methods

    //  TODO: Add specific properties for your application
    unsigned char *broker_pk;
    unsigned char *client_pk;
    unsigned char *session_key_rx;
    unsigned char *session_key_tx;
} client_t;

//  Include the generated client engine
#include "mdp_client_engine.inc"

//  Allocate properties and structures for a new client instance.
//  Return 0 if OK, -1 if failed

static int
client_initialize(client_t *self) {
    return 0;
}

//  Free properties and structures for a client instance

static void
client_terminate(client_t *self) {
    //  Destroy properties here
}


//  ---------------------------------------------------------------------------
//  Selftest

void
mdp_client_test(bool verbose) {
    printf(" * mdp_client: ");
    if (verbose)
        printf("\n");

    //  @selftest
    zactor_t *client = zactor_new(mdp_client, NULL);
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
//  signal_connection_success
//

static void
signal_connection_success(client_t *self) {

}


//  ---------------------------------------------------------------------------
//  send_request_to_broker
//

static void
send_request_to_broker(client_t *self) {
    mdp_client_msg_t *msg;

    msg = mdp_client_msg_new();
    assert(msg);
    mdp_client_msg_set_id(msg, MDP_CLIENT_MSG_CLIENT_REQUEST);
    mdp_client_msg_set_service(msg, self->args->service);
    // stack 3 frames to the original body: #1 PLAIN/SECURE Identifier, #2 client ephemeral PK, nonce, encrypted body
    if (NULL != self->broker_pk) {
        if (NULL == self->client_pk) {
            // generate
            unsigned char client_sk[crypto_kx_SECRETKEYBYTES];
            self->client_pk = (unsigned char *) zmalloc(crypto_kx_PUBLICKEYBYTES);

            // generate an ephemeral keypair for the key exchange
            if (crypto_kx_keypair(self->client_pk, (unsigned char *) &client_sk)) {
                zsys_error("Failed to generate Public/private key pairs");
                free(self->client_pk);
                self->client_pk = NULL;
            } else {
                // base64 decode the broker PK
                unsigned char broker_pk[crypto_kx_PUBLICKEYBYTES];
                size_t binlen = 0;
                sodium_base642bin(broker_pk, crypto_kx_PUBLICKEYBYTES, (const char *) self->broker_pk,
                                  strlen((char *) self->broker_pk),
                                  NULL, &binlen, NULL, sodium_base64_VARIANT_URLSAFE_NO_PADDING);

                // generate session keys
                self->session_key_tx = (unsigned char *) zmalloc(crypto_kx_SESSIONKEYBYTES);
                self->session_key_rx = (unsigned char *) zmalloc(crypto_kx_SESSIONKEYBYTES);
                if (crypto_kx_client_session_keys(self->session_key_rx, self->session_key_tx, self->client_pk,
                                                  client_sk, broker_pk)) {
                    zsys_error("Failed to generate session keys");
                    if (self->session_key_tx) {
                        free(self->session_key_tx);
                        self->session_key_tx = NULL;
                    }
                    if (self->session_key_rx) {
                        free(self->session_key_rx);
                        self->session_key_rx = NULL;
                    }
                    free(self->client_pk);
                    self->client_pk = NULL;
                }
            }

            // encrypt the original body - frame by frame
            unsigned char nonce[crypto_secretbox_NONCEBYTES];
            int num_frames = (int) zmsg_size(self->args->body);
            int i = 0;
            for (i = 0; i < num_frames; i++) {
                zframe_t *frame = zmsg_pop(self->args->body);
                if (NULL != frame) {
                    randombytes_buf(nonce, crypto_secretbox_NONCEBYTES);
                    size_t data_plain_len = zframe_size(frame);
                    unsigned char *data_plain = (unsigned char *) zframe_data(frame);
                    size_t data_encrypted_len = crypto_secretbox_MACBYTES + data_plain_len;
                    unsigned char *data_encrypted = (unsigned char *) zmalloc(data_encrypted_len);
                    if (NULL == data_encrypted) {
                        zsys_error("Memory allocation error");
                        return;
                    }
                    int res = crypto_secretbox_easy(data_encrypted, data_plain,
                                                    data_plain_len,
                                                    nonce, self->session_key_tx);
                    zsys_debug("Encrypting frame %d - %s", i + 1, res == 0 ? "SUCESS" : "ERROR");
                    if (res != 0) {
                        return;
                    }
                    zmsg_addmem(self->args->body, nonce, crypto_secretbox_NONCEBYTES);
                    zmsg_addmem(self->args->body, data_encrypted, data_encrypted_len);
                    zframe_destroy(&frame);

                }
            }

            // prepend identifier pubkey and frames
            zmsg_pushmem(self->args->body, self->client_pk, crypto_kx_PUBLICKEYBYTES);
            zmsg_pushstr(self->args->body, "BB_MDP_SECURE");
        }
    } else {
        // in PLAIN mode, just add the BB_MDP_PLAINTEXT identifier frame
        zmsg_pushstr(self->args->body, "BB_MDP_PLAIN");
    }

    mdp_client_msg_set_body(msg, &self->args->body);
    mdp_client_msg_send(msg, self->dealer);
    mdp_client_msg_destroy(&msg);

}


//  ---------------------------------------------------------------------------
//  disconnect_from_broker
//

static void
disconnect_from_broker(client_t *self) {

}

static int s_decrypt_body(zmsg_t *body, unsigned char *key) {
    //decrypt frames one by one
    int i;
    int numframes = (int) zmsg_size(body);
    unsigned char nonce[crypto_secretbox_NONCEBYTES];
    for (i = 0; i < numframes; i += 2) {
        // net nonce
        zframe_t *f = zmsg_pop(body);
        if (f) {
            memcpy(nonce, zframe_data(f), crypto_secretbox_NONCEBYTES);
            zframe_destroy(&f);
            f = zmsg_pop(body);
            if (f) {
                unsigned char *frame_encrypted = zframe_data(f);
                size_t frame_encrypted_len = zframe_size(f);
                size_t frame_plain_len = frame_encrypted_len - crypto_secretbox_MACBYTES;
                unsigned char *frame_plain = (unsigned char *) zmalloc(frame_plain_len);
                if (frame_plain) {
                    int res = crypto_secretbox_open_easy(frame_plain, frame_encrypted, frame_encrypted_len,
                                                         nonce, key);
                    if (0 == res) {
                        zsys_debug("Decrypted frame #%d", i);
                        zmsg_addmem(body, frame_plain, frame_plain_len);
                    }
                } else {
                    return -1;
                }
                zframe_destroy(&f);
            } else {
                return -1;
            }
        } else {
            return -1;
        }
    }
    return 0;
}


//  ---------------------------------------------------------------------------
//  send_partial_response
//

static void
send_partial_response(client_t *self) {
    zmsg_t *body = mdp_client_msg_get_body(self->message);
    // Check if the response is encrypted
    zframe_t *frame = zmsg_pop(body);
    if (frame) {
        if (zframe_streq(frame, "BB_MDP_SECURE")) {
            zsys_debug("Encrypted message");
            s_decrypt_body(body, self->session_key_rx);
        } else if (zframe_streq(frame, "BB_MDP_PLAIN")) {
            zsys_debug("Plain message");
        } else {
            zsys_error("Invalid message (missing security identifier)");
            zframe_destroy(&frame);
            return;
        }
        zframe_destroy(&frame);
    }

    zsock_send(self->msgpipe, "sm", "PARTIAL", body);
}


//  ---------------------------------------------------------------------------
//  send_final_response
//

static void
send_final_response(client_t *self) {
    zmsg_t *body = mdp_client_msg_get_body(self->message);
    // Check if the response is encrypted
    zframe_t *frame = zmsg_pop(body);
    if (frame) {
        if (zframe_streq(frame, "BB_MDP_SECURE")) {
            zsys_debug("Encrypted message");
            s_decrypt_body(body, self->session_key_rx);
        } else if (zframe_streq(frame, "BB_MDP_PLAIN")) {
            zsys_debug("Plain message");
        } else {
            zsys_error("Invalid message (missing security identifier)");
            zframe_destroy(&frame);
            return;
        }
        zframe_destroy(&frame);
    }
    zsock_send(self->msgpipe, "sm", "FINAL", body);
}


//  ---------------------------------------------------------------------------
//  log_protocol_error
//

static void
log_protocol_error(client_t *self) {
    zsys_error("** protocol error **");
    mdp_client_msg_print(self->message);
}


//  ---------------------------------------------------------------------------
//  signal_connect_error
//

static void
signal_connect_error(client_t *self) {
    zsock_send(self->cmdpipe, "si", "CONNECT ERROR", 0);
}


//  ---------------------------------------------------------------------------
//  handle_set_verbose
//

static void
handle_set_verbose(client_t *self) {
    mdp_client_verbose = true;
}
