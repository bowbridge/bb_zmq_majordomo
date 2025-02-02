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

// SECURE_MDP functions:

static int s_client_encrypt_body_frames(zmsg_t *body, unsigned char *key) {
    int rc = -1;
    if (NULL != body) {
        if (NULL != key) {
            // encrypt the original body - frame by frame
            int num_frames = (int) zmsg_size(body);

            // create random nonce
            unsigned char initial_nonce[crypto_secretbox_NONCEBYTES];
            randombytes_buf(initial_nonce, crypto_secretbox_NONCEBYTES);

            // store the initial nonce, as we need it when stacking the massage
            unsigned char nonce[crypto_secretbox_NONCEBYTES];
            memcpy(nonce, initial_nonce, crypto_secretbox_NONCEBYTES);

            int i = 0;
            //zsys_debug("CLIENT: Encrypting with key %2x %2x ... %2x %2x ", key[0], key[1],key[crypto_kx_SESSIONKEYBYTES - 2], key[crypto_kx_SESSIONKEYBYTES - 1]);
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
                    // zsys_debug("BROKER: Encrypting frame %d - %s", i + 1, res == 0 ? "SUCESS" : "ERROR");
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
            crypto_secretbox_easy(data_encrypted, (unsigned char *) canary,
                                  strlen(canary),
                                  nonce, key);
            zmsg_addmem(body, data_encrypted, strlen(canary) + crypto_secretbox_MACBYTES);
            free(data_encrypted);

            zmsg_pushmem(body, initial_nonce, crypto_secretbox_NONCEBYTES);
            zmsg_pushstr(body, "BB_MDP_SECURE");
            rc = 0;
        } else {
            // prepend identifier pubkey and frames
            rc = zmsg_pushstr(body, "BB_MDP_PLAIN");
        }

    }
    return rc;
}


static int s_client_encrypt_body(client_t *self) {
    int rc = -1;
    int res;
    // stack 3 frames to the original body: #1 PLAIN/SECURE Identifier, #2 client ephemeral PK, nonce, encrypted body
    if (NULL != self->broker_pk_b64) {
        if (NULL == self->client_pk || NULL == self->client_sk) {
            // generate new keys as they don't exist for this session
            self->client_pk = (unsigned char *) zmalloc(crypto_kx_PUBLICKEYBYTES);
            self->client_sk = (unsigned char *) zmalloc(crypto_kx_SECRETKEYBYTES);

            // generate an ephemeral keypair for the key exchange
            res = crypto_kx_keypair(self->client_pk, self->client_sk);
            if (0 == res) {}
            else {
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

        zmsg_t *body = self->args->body;
        if (0 == s_client_encrypt_body_frames(body, self->session_key_tx)) {
            zframe_t *encryption_indicator = zmsg_first(body);
            if (zframe_streq(encryption_indicator, "BB_MDP_SECURE")) {
                // if the body was encrypted, insert the client pubkey between the indicator and the nonce
                zframe_t *f = zmsg_pop(body); // remove the indicator
                zframe_destroy(&f); // discard
                zmsg_pushmem(body, self->client_pk, crypto_kx_PUBLICKEYBYTES); // add key on top
                zmsg_pushstr(body, "BB_MDP_SECURE"); // re-add the indicator
                rc = 0;
            } else {
                zsys_error("CLIENT: Encrypting frames failed");
            }
        }
    } else {
        // in PLAIN mode, just add the BB_MDP_PLAINTEXT identifier frame
        zmsg_pushstr(self->args->body, "BB_MDP_PLAIN");
        rc = 0;
    }
    return rc;
}

static int s_client_decrypt_body(zmsg_t *body, unsigned char *key) {
    int rc = -1;
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
            // zsys_debug("CLIENT: Decrypting with key %2x %2x ... %2x %2x ", key[0], key[1],key[crypto_kx_SESSIONKEYBYTES - 2], key[crypto_kx_SESSIONKEYBYTES - 1]);
            int errror_count = 0;
            for (i = 0; i < num_frames; i++) {

                frame = zmsg_pop(body);
                if (frame) {
                    unsigned char *ciphertext = zframe_data(frame);
                    if (ciphertext) {
                        size_t ciphertextlen = zframe_size(frame);
                        size_t plaintextlen = ciphertextlen - crypto_secretbox_MACBYTES;
                        unsigned char *plaintext = (unsigned char *) zmalloc(plaintextlen);
                        if (plaintext) {
                            if (0 == crypto_secretbox_open_easy(plaintext, ciphertext,
                                                                (unsigned long long int) ciphertextlen, nonce,
                                                                key)) {
                                zmsg_addmem(body, plaintext, plaintextlen);
                                free(plaintext);

                                // zsys_debug("WORKER: decrypted data frame #%d", (i + 1));
                                // increment the nonce for the next frame (if any)
                                sodium_increment(nonce, crypto_secretbox_NONCEBYTES);
                            } else {
                                errror_count++;
                            }
                        }
                    }
                    zframe_destroy(&frame);
                }
            }
            if (0 == errror_count) {
                // get/decrypt "Canary" frame
                frame = zmsg_pop(body);
                size_t ciphertextlen = zframe_size(frame);
                size_t plaintextlen = ciphertextlen - crypto_secretbox_MACBYTES;
                unsigned char *plaintext = (unsigned char *) zmalloc(plaintextlen);
                int res = crypto_secretbox_open_easy(plaintext, zframe_data(frame),
                                                     (unsigned long long int) ciphertextlen, nonce,
                                                     key);
                zframe_destroy(&frame);
                if (0 == res &&
                    0 == memcmp(plaintext, "BB_MDP_SECURE", strlen("BB_MDP_SECURE"))) {
                    rc = 0;
                } else {
                    zsys_error("CLIENT: Decryption error - Check failed");
                    zmsg_destroy(&body);

                }
                free(plaintext);
            } else {
                zsys_error("CLIENT: Failed to decrypt at least one data frame");
            }
        } else {
            zsys_error("CLIENT: malformed body: no data frame");
        }
    }
    return rc;
}



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
    if (0 == s_client_encrypt_body(self)) {
        mdp_client_msg_t *msg;
        msg = mdp_client_msg_new();
        assert(msg);
        mdp_client_msg_set_id(msg, MDP_CLIENT_MSG_CLIENT_REQUEST);
        mdp_client_msg_set_service(msg, self->args->service);

        mdp_client_msg_set_body(msg, &self->args->body);
        mdp_client_msg_send(msg, self->dealer);
        mdp_client_msg_destroy(&msg);
    }
}


//  ---------------------------------------------------------------------------
//  disconnect_from_broker
//

static void
disconnect_from_broker(client_t *self) {

}


//  ---------------------------------------------------------------------------
//  send_partial_response
//

static void
send_partial_response(client_t *self) {
    zmsg_t *body = mdp_client_msg_body(self->message);
    // Check if the response is encrypted
    zframe_t *frame = zmsg_pop(body);
    if (frame) {
        if (zframe_streq(frame, "BB_MDP_SECURE")) {
            //  zsys_debug("CLIENT: message is ENCRYPTED");
            s_client_decrypt_body(body, self->session_key_rx);
        } else if (zframe_streq(frame, "BB_MDP_PLAIN")) {
            // zsys_debug("CLIENT: message is PLAIN");
        } else {
            zsys_error("Invalid partial response message (missing security identifier)");
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
    zmsg_t *body = mdp_client_msg_body(self->message);
    // Check if the response is encrypted
    zframe_t *frame = zmsg_pop(body);
    if (frame) {
        if (zframe_streq(frame, "BB_MDP_SECURE")) {
            // zsys_debug("CLIENT: got Encrypted message");
            s_client_decrypt_body(body, self->session_key_rx);
        } else if (zframe_streq(frame, "BB_MDP_PLAIN")) {
            //  zsys_debug("CLIENT: got Plain message");
        } else {
            zsys_error("Invalid final response message (missing security identifier: %s)", zframe_strhex(frame));
            zmsg_dump(body);
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
