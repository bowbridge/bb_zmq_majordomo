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
    unsigned char *broker_pk_b64;
    unsigned char *broker_pk_bin;
    unsigned char *client_pk;
    unsigned char *client_sk;
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
        zsys_warning("could not connect client to %s", self->args->endpoint);
        zsock_send(self->cmdpipe, "si", "FAILURE", 0);
    } else {
        // zsys_debug("connected to %s", self->args->endpoint);
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
    int res;

    msg = mdp_client_msg_new();
    assert(msg);
    mdp_client_msg_set_id(msg, MDP_CLIENT_MSG_CLIENT_REQUEST);
    mdp_client_msg_set_service(msg, self->args->service);
    // stack 3 frames to the original body: #1 PLAIN/SECURE Identifier, #2 client ephemeral PK, nonce, encrypted body
    if (NULL != self->broker_pk_b64) {
        if (NULL == self->client_pk || NULL == self->client_sk) {
            // generate new keys as they don't exist for this session
            self->client_pk = (unsigned char *) zmalloc(crypto_kx_PUBLICKEYBYTES);
            self->client_sk = (unsigned char *) zmalloc(crypto_kx_SECRETKEYBYTES);

            // generate an ephemeral keypair for the key exchange
            res = crypto_kx_keypair(self->client_pk, self->client_sk);
            if (0 != res) {
                zsys_error("Failed to generate public/private key pair");
                free(self->client_pk);
                self->client_pk = NULL;
                free(self->client_sk);
                self->client_sk = NULL;
            }

            // base64 decode the broker PK
            if (NULL == self->broker_pk_bin) {
                self->broker_pk_bin = (unsigned char *) zmalloc(crypto_kx_PUBLICKEYBYTES);
                size_t binlen = 0;
                sodium_base642bin(self->broker_pk_bin, crypto_kx_PUBLICKEYBYTES, (const char *) self->broker_pk_b64,
                                  strlen((char *) self->broker_pk_b64),
                                  NULL, &binlen, NULL, sodium_base64_VARIANT_URLSAFE_NO_PADDING);
            }

            // generate session keys
            self->session_key_tx = (unsigned char *) zmalloc(crypto_kx_SESSIONKEYBYTES);
            self->session_key_rx = (unsigned char *) zmalloc(crypto_kx_SESSIONKEYBYTES);
            if (crypto_kx_client_session_keys(self->session_key_rx, self->session_key_tx, self->client_pk,
                                              self->client_sk, self->broker_pk_bin)) {
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
                free(self->client_sk);
                self->client_sk = NULL;
            }
        }


        // encrypt the original body - frame by frame
        unsigned char initial_nonce[crypto_secretbox_NONCEBYTES];
        randombytes_buf(initial_nonce, crypto_secretbox_NONCEBYTES);
        unsigned char nonce[crypto_secretbox_NONCEBYTES];
        memcpy(nonce, initial_nonce, crypto_secretbox_NONCEBYTES);

        int num_frames = (int) zmsg_size(self->args->body);
        int i = 0;
        zmsg_t *body_encrypted = zmsg_new();
        if (body_encrypted) {
            for (i = 0; i < num_frames; i++) {
                zframe_t *frame = zmsg_pop(self->args->body);
                if (NULL != frame) {
                    size_t data_plain_len = zframe_size(frame);
                    unsigned char *data_plain = (unsigned char *) zframe_data(frame);
                    size_t data_encrypted_len = crypto_secretbox_MACBYTES + data_plain_len;
                    unsigned char *data_encrypted = (unsigned char *) zmalloc(data_encrypted_len);
                    if (data_encrypted) {
                        zsys_debug("CLIENT: Encrypting with key %2x %2x ... %2x %2x ", self->session_key_tx[0],
                                   self->session_key_tx[1], self->session_key_tx[crypto_kx_SESSIONKEYBYTES - 2],
                                   self->session_key_tx[crypto_kx_SESSIONKEYBYTES - 1]);
                        res = crypto_secretbox_easy(data_encrypted, data_plain,
                                                    data_plain_len,
                                                    nonce, self->session_key_tx);
                        zsys_debug("CLIENT: Encrypting frame %d - %s", i + 1, res == 0 ? "SUCESS" : "ERROR");
                        if (0 == res) {
                            zmsg_addmem(body_encrypted, data_encrypted, data_encrypted_len);
                            free(data_encrypted);
                        } else {
                            zsys_error("Encryption error");
                        }
                    } else {
                        zsys_error("Memory allocation error");
                        res = -1;
                    }
                    zframe_destroy(&frame);
                    if (res != 0) {
                        // encryption error
                        return;
                    }
                    // increment the nonce for the next frame (if any)
                    sodium_increment(nonce, crypto_secretbox_NONCEBYTES);
                }
            }

            // add the "canary" frame
            char *canary = "BB_MDP_SECURE";
            unsigned char *data_encrypted = (unsigned char *) zmalloc(strlen(canary) + crypto_secretbox_MACBYTES);

            crypto_secretbox_easy(data_encrypted, (unsigned char *) canary,
                                  strlen(canary),
                                  nonce, self->session_key_tx);
            zmsg_addmem(body_encrypted, data_encrypted, strlen(canary) + crypto_secretbox_MACBYTES);
            free(data_encrypted);


            // prepend identifier pubkey and first nonce
            zmsg_pushmem(body_encrypted, initial_nonce, crypto_secretbox_NONCEBYTES);
            zmsg_pushmem(body_encrypted, self->client_pk, crypto_kx_PUBLICKEYBYTES);
            zmsg_pushstr(body_encrypted, "BB_MDP_SECURE");

            // destroy original body
            zmsg_destroy(&self->args->body);
            self->args->body = body_encrypted;

        } else {
            // in PLAIN mode, just add the BB_MDP_PLAINTEXT identifier frame
            zmsg_pushstr(self->args->body, "BB_MDP_PLAIN");
        }
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

static zmsg_t *s_client_decrypt_body(mdp_client_msg_t *msg, unsigned char *key) {
    int i;
    zmsg_t *body = mdp_client_msg_body(msg);
    // Check if the response is encrypted
    zframe_t *security_id = zmsg_pop(body);
    if (security_id) {
        if (zframe_streq(security_id, "BB_MDP_SECURE")) {
            //decrypt frames one by one
            unsigned char nonce[crypto_secretbox_NONCEBYTES];
            zframe_t *f = zmsg_pop(body);
            if (f) {
                memcpy(nonce, zframe_data(f), crypto_secretbox_NONCEBYTES);
                zframe_destroy(&f);
                int numframes = (int) zmsg_size(body) - 1;
                /*zsys_debug("********** CLIENT: Decrypting %d frames  with key %2x %2x ... %2x %2x ", numframes, key[0], key[1],
                           key[crypto_kx_SESSIONKEYBYTES - 2], key[crypto_kx_SESSIONKEYBYTES - 1]);*/
                for (i = 0; i < numframes; i++) {
                    f = zmsg_pop(body);
                    if (f) {
                        unsigned char *frame_encrypted = zframe_data(f);
                        size_t frame_encrypted_len = zframe_size(f);
                        size_t frame_plain_len = frame_encrypted_len - crypto_secretbox_MACBYTES;
                        unsigned char *frame_plain = (unsigned char *) zmalloc(frame_plain_len);
                        if (frame_plain) {
                            if (0 == crypto_secretbox_open_easy(frame_plain, frame_encrypted, frame_encrypted_len,
                                                                nonce, key)) {
                                zsys_debug("********** Decrypted frame #%d", i);
                                zmsg_addmem(body, frame_plain, frame_plain_len);
                            } else {
                                zsys_error("Decryption failed!");
                            }
                            free(frame_plain);
                            frame_plain = NULL;
                        } else {
                            zmsg_destroy(&body);
                            return NULL;
                        }
                        zframe_destroy(&f);
                        // increment the nonce for the next frame (if any)
                        sodium_increment(nonce, crypto_secretbox_NONCEBYTES);
                    } else {
                        return NULL;
                    }
                }

                // get/decrypt "Canary" frame
                f = zmsg_pop(body);
                size_t ciphertextlen = zframe_size(f);
                size_t plaintextlen = ciphertextlen - crypto_secretbox_MACBYTES;
                unsigned char *plaintext = (unsigned char *) zmalloc(plaintextlen);
                int res = crypto_secretbox_open_easy(plaintext, zframe_data(f),
                                                     (unsigned long long int) ciphertextlen, nonce,
                                                     key);
                if (0 != res ||
                    0 != memcmp(plaintext, "BB_MDP_SECURE", strlen("BB_MDP_SECURE"))) {
                    zsys_error("CLIENT: Decryption error - Check failed");
                    zmsg_destroy(&body);
                }
                if (plaintext) {
                    free(plaintext);
                }
                zframe_destroy(&f);

            } else {
                zmsg_destroy(&body);
            }
        } else {
            //  zsys_debug("CLIENT: got Plain message");
        }
    }
    return body;
}


//  ---------------------------------------------------------------------------
//  send_partial_response
//

static void
send_partial_response(client_t *self) {
    zmsg_t *body = s_client_decrypt_body(self->message, self->session_key_rx);
    zsock_send(self->msgpipe, "sm", "PARTIAL", body);
}


//  ---------------------------------------------------------------------------
//  send_final_response
//

static void
send_final_response(client_t *self) {
    zmsg_t *body = s_client_decrypt_body(self->message, self->session_key_rx);
    //zmsg_t *body = mdp_client_msg_get_body(self->message);
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
