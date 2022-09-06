/*  =========================================================================
    mdp_broker - mdp_broker

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
#include "../include/mdp_msg.h"
#include "../include/mdp_broker.h"

//  ---------------------------------------------------------------------------
//  Forward declarations for the two main classes we use here

typedef struct _server_t server_t;
typedef struct _client_t client_t;

//  This structure defines the context for each running server. Store
//  whatever properties and structures you need for the server.

struct _server_t {
    //  These properties must always be present in the server_t
    //  and are set by the generated engine; do not modify them!
    zsock_t *pipe;              //  Actor pipe back to caller
    zconfig_t *config;          //  Current loaded configuration

    //  TODO: Add any properties you need here
    zhash_t *services;      // Hash of known services
    zhash_t *workers;       // Hash of known workers
    zhash_t *clients;      // list of known clients
    zlist_t *waiting;       // List of waiting workers
    zsock_t *router;        // The same socket as router in s_server_t
    zlist_t *known_psks;
    unsigned char *my_pk;
    unsigned char *my_sk;
};


// The service class defines a single service instance.
typedef struct {
    server_t *broker;       // Broker instance
    char *name;             // Service name
    zlist_t *requests;      // List of client requests
    zlist_t *waiting;       // List of waiting workers
    size_t workers;         // How many workers we have
} service_t;


// The worker class defines a single worker, idle or active
typedef struct {
    server_t *broker;      // Broker instance
    char *identity;         // Identity of worker
    zframe_t *address;      // Address frame to route to
    service_t *service;     // Owning service, if known
    int64_t expiry;         // Expires at unless heartbeat
    unsigned char *session_key_tx;
    unsigned char *session_key_rx;
} worker_t;


//  ---------------------------------------------------------------------------
//  This structure defines the state for each client connection. It will
//  be passed to each action in the 'self' argument.
struct _client_t {
    //  These properties must always be present in the client_t
    //  and are set by the generated engine; do not modify them!
    server_t *server;           //  Reference to parent server
    mdp_msg_t *message;         //  Message in and out

    //  TODO: Add specific properties for your application
    unsigned int timeouts;      // Number of timeouts
    char *service_name;         // Service name called by client request
    unsigned char *client_pk;
    unsigned char *session_key_rx;
    unsigned char *session_key_tx;
    worker_t *worker; // worker using this client connection
};




//  Include the generated server engine
#include "mdp_broker_engine.inc"

// Maximum number of timeouts; If this number is reached, we stop sending
// heartbeats and terminate connection.
#define MAX_TIMEOUTS 7

// Interval for sending heartbeat [ms]
#define HEARTBEAT_DELAY 1500

static void s_service_destroy(void *argument);

static void s_service_dispatch(service_t *self);

// Worker destructor is called automatically whenever the worker is
// removed from broker->workers.

static void s_worker_destroy(void *argument);

static void s_worker_delete(worker_t *self, int disconnect);

static int s_broker_encrypt_body_frames(zmsg_t *body, unsigned char *key);

static int s_broker_decrypt_body_frames(zmsg_t *body, unsigned char *key);

static worker_t *
s_worker_require(server_t *self, zframe_t *address) {
    assert(address);

    // self->workers is keyed off worker identity.
    char *identity = zframe_strhex(address);
    worker_t *worker =
            (worker_t *) zhash_lookup(self->workers, identity);

    if (worker == NULL) {
        worker = (worker_t *) zmalloc(sizeof(worker_t));
        worker->broker = self;
        worker->identity = identity;
        worker->address = zframe_dup(address);

        zhash_insert(self->workers, identity, worker);
        zhash_freefn(self->workers, identity, s_worker_destroy);
        // zsys_debug("Worker %s created", worker->identity);
    } else {
        zsys_warning("Worker %s existed", identity);
        free(identity);
    }
    return worker;
}

static void
s_worker_destroy(void *argument) {
    worker_t *self = (worker_t *) argument;
    zframe_destroy(&self->address);
    // zsys_debug("destroying worker %s", self->identity);
    free(self->identity);
    if (self->session_key_tx)
        free(self->session_key_tx);
    if (self->session_key_rx)
        free(self->session_key_rx);
    free(self);
}

static void
s_worker_delete(worker_t *self, int disconnect) {
    assert(self);
    if (disconnect) {
        mdp_msg_t *msg = mdp_msg_new();
        assert(msg);
        mdp_msg_set_id(msg, MDP_MSG_DISCONNECT);
        mdp_msg_set_routing_id(msg, self->address);
        mdp_msg_send(msg, self->broker->router);
    }

    if (self->service) {
        // zsys_debug("Deleting worker %s from service list", self->identity);
        zlist_remove(self->service->waiting, self);
        self->service->workers--;
    }
    // zsys_debug("Deleting worker %s from broker's waiting list", self->identity);
    zlist_remove(self->broker->waiting, self);
    // This implicitly calls s_worker_destroy.
    zhash_delete(self->broker->workers, self->identity);
}

static service_t *s_service_require(server_t *self, const char *service_name);

static service_t *
s_service_require(server_t *self, const char *service_name) {
    char *name = strdup(service_name);
    service_t *service = (service_t *) zhash_lookup(self->services, name);
    if (service == NULL) {
        service = (service_t *) zmalloc(sizeof(service_t));
        service->broker = self;
        service->name = name;
        service->requests = zlist_new();
        service->waiting = zlist_new();
        zhash_insert(self->services, name, service);
        zhash_freefn(self->services, name, s_service_destroy);
    } else
        zstr_free(&name);
    return service;
}

/*
static worker_t *s_get_random_worker(zlist_t *list) {
    size_t elements = zlist_size(list);
    int index = (int) (rand() % elements) - 1;
    worker_t *worker = (worker_t *) zlist_first(list);
    while (NULL != worker && --index > 0) {
        worker = (worker_t *) zlist_next(list);
    }
    return worker;
}
 */

static void
s_service_dispatch(service_t *self) {
    while ((zlist_size(self->requests) > 0) &&
           (zlist_size(self->waiting) > 0)) {
        //worker_t *worker = s_get_random_worker(self->waiting);
        worker_t *worker = (worker_t *) zlist_pop(self->waiting);
        zlist_remove(self->broker->waiting, worker);
        mdp_msg_t *msg = (mdp_msg_t *) zlist_pop(self->requests);
        mdp_msg_t *worker_msg = mdp_msg_new();
        mdp_msg_set_id(worker_msg, MDP_MSG_WORKER_REQUEST);
        mdp_msg_set_routing_id(worker_msg, worker->address);
        zframe_t *address = zframe_dup(mdp_msg_routing_id(msg));
        mdp_msg_set_address(worker_msg, &address);
        zmsg_t *body = mdp_msg_get_body(msg);
        // encrypt before sending to the worker
        s_broker_encrypt_body_frames(body, worker->session_key_tx);
        mdp_msg_set_body(worker_msg, &body);
        // zsys_debug("BROKER: Dispatching request to worker %s", worker->identity);
        mdp_msg_send(worker_msg, self->broker->router);
        mdp_msg_destroy(&worker_msg);
        mdp_msg_destroy(&msg);
    }
}

// Service destructor is called automatically whenever the service is
// removed from broker->services.

static void
s_service_destroy(void *argument) {
    service_t *service = (service_t *) argument;
    while (zlist_size(service->requests) > 0) {
        zmsg_t *msg = (zmsg_t *) zlist_pop(service->requests);
        zmsg_destroy(&msg);
    }
    zlist_destroy(&service->requests);
    zlist_destroy(&service->waiting);
    free(service->name);
    free(service);
}

//  Allocate properties and structures for a new server instance.
//  Return 0 if OK, or -1 if there was an error.

static int
server_initialize(server_t *self) {
    //  Construct properties here
    self->services = zhash_new();
    self->workers = zhash_new();
    self->waiting = zlist_new();
    s_server_t *server = (s_server_t *) self;
    self->router = server->router;
    return 0;
}

//  Free properties and structures for a server instance

static void
server_terminate(server_t *self) {
    //  Destroy properties here
    if (self->my_sk)
        free(self->my_sk);
    if (self->my_pk)
        free(self->my_pk);
    zlist_destroy(&self->waiting);
    if (self->known_psks) {
        zlist_destroy(&self->known_psks);
        self->known_psks = NULL;
    }
    zhash_destroy(&self->workers);
    zhash_destroy(&self->services);
}

//  Process server API method, return reply message if any

static zmsg_t *
server_method(server_t *self, const char *method, zmsg_t *msg) {
    return NULL;
}


//  Allocate properties and structures for a new client connection and
//  optionally engine_set_next_event (). Return 0 if OK, or -1 on error.

static int
client_initialize(client_t *self) {
    //  Construct properties here
    self->timeouts = 0;
    // Client init service
    const char *msg_service = mdp_msg_service(self->message);
    if (msg_service != NULL) {
        self->service_name = (char *) zmalloc((strlen(msg_service) + 1) * sizeof(char));
        assert(self->service_name);
        snprintf(self->service_name, strlen(msg_service) + 1, "%s", msg_service);
    }
    return 0;
}

//  Free properties and structures for a client connection

static void
client_terminate(client_t *self) {
    //  Destroy properties here
    free(self->service_name);
    if (self->session_key_tx)
        free(self->session_key_tx);
    if (self->session_key_rx)
        free(self->session_key_rx);
    if (self->client_pk)
        free(self->client_pk);
}

//  ---------------------------------------------------------------------------
//  handle_mmi
//

static void
handle_mmi(client_t *self, const char *service_name) {

    char *result = zsys_sprintf("501");
    zmsg_t *mmibody = mdp_msg_get_body(self->message);

    if (mmibody) {
        if (strstr(service_name, "mmi.service")) {
            char *svc_lookup = zmsg_popstr(mmibody);
            if (svc_lookup) {
                service_t *service = (service_t *) zhash_lookup(self->server->services, svc_lookup);
                if (service && service->workers) {
                    zstr_free(&result);
                    result = zsys_sprintf("200");
                } else {
                    zstr_free(&result);
                    result = zsys_sprintf("404");
                }
                zstr_free(&svc_lookup);
            }
        }


        if (strstr(service_name, "mmi.status")) {
            char *svc = zmsg_popstr(mmibody);
            if (streq(svc, "all") || streq(svc, "raw") || streq(svc, "json")) {
                service_t *service = (service_t *) zhash_first(self->server->services);
                char *oldresult = NULL;
                if (streq(svc, "json")) {
                    oldresult = zsys_sprintf("{");
                } else {
                    oldresult = zsys_sprintf("");
                }
                while (service) {
                    if (streq(svc, "raw")) {
                        result = zsys_sprintf("%s%s;%d;%d;%d\n", oldresult, service->name,
                                              service->workers,
                                              zlist_size(service->waiting), zlist_size(service->requests));
                    } else if (streq(svc, "json")) {
                        result = zsys_sprintf(
                                "%s \"%s\":{\"workers\":\"%d\", \"waiting\":\"%d\", \"requests-queued\":\"%d\"}",
                                oldresult, service->name,
                                service->workers,
                                zlist_size(service->waiting), zlist_size(service->requests));
                    } else {
                        result = zsys_sprintf("%s%s: %d active, %d waiting, %d requests queued\n", oldresult,
                                              service->name,
                                              service->workers,
                                              zlist_size(service->waiting), zlist_size(service->requests));
                    }
                    zstr_free(&oldresult);
                    oldresult = result;
                    service = (service_t *) zhash_next(self->server->services);
                    if (streq(svc, "json")) {
                        if (NULL != service) {
                            result = zsys_sprintf("%s, ", oldresult);
                        } else {
                            result = zsys_sprintf("%s }", oldresult);
                        }
                        zstr_free(&oldresult);
                        oldresult = result;
                    }
                }
            } else {
                service_t *service = (service_t *) zhash_lookup(self->server->services, svc);
                if (service) {
                    zstr_free(&result);
                    result = zsys_sprintf("%d", zlist_size(service->waiting));
                } else {
                    zstr_free(&result);
                    result = zsys_sprintf("404");

                }
                zstr_free(&svc);

            }
        }

        zmsg_destroy(&mmibody); // no longer needed
    }

    // Set routing id, messageid, service, body
    mdp_msg_t *client_msg = mdp_msg_new();
    mdp_msg_set_routing_id(client_msg, mdp_msg_routing_id(self->message));
    mdp_msg_set_id(client_msg, MDP_MSG_CLIENT_FINAL);
    mdp_msg_set_service(client_msg, service_name);
    zmsg_t *rep_body = zmsg_new();
    zmsg_pushstr(rep_body, result);
    zstr_free(&result);
    s_broker_encrypt_body_frames(rep_body, self->session_key_tx);
    mdp_msg_set_body(client_msg, &rep_body);
    mdp_msg_send(client_msg, self->server->router);
    mdp_msg_destroy(&client_msg);
}

static worker_t *s_get_worker_by_routing_ID(zhash_t *workers, zframe_t *address) {
    char *identity = zframe_strhex(address);
    worker_t *worker =
            (worker_t *) zhash_lookup(workers, identity);
    zstr_free(&identity);
    return worker;
}

static int s_broker_encrypt_body_frames(zmsg_t *body, unsigned char *key) {
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
            // zsys_debug("BROKER: Encrypting with key %2x %2x ... %2x %2x ", key[0], key[1],key[crypto_kx_SESSIONKEYBYTES - 2], key[crypto_kx_SESSIONKEYBYTES - 1]);
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

static int s_broker_encrypt_client_msg(client_t *client, mdp_msg_t *msg) {
    // get the client's key
    char *hashkey = zframe_strhex(mdp_msg_routing_id(msg));
    s_client_t *s_client = (s_client_t *) zhash_lookup(client->server->clients, hashkey);
    zstr_free(&hashkey);
    unsigned char *key = s_client->client.session_key_tx;
    int rc = s_broker_encrypt_body_frames(mdp_msg_body(msg), key);

    return rc;
}

static int s_broker_decrypt_body_frames(zmsg_t *body, unsigned char *key) {
    int rc = -1;
    if (NULL != body && NULL != key) {
        // decrypt the body, frame by frame
        unsigned char nonce[crypto_secretbox_NONCEBYTES];
        zframe_t *nonce_frame = zmsg_pop(body);
        if (nonce_frame) {
            // nonce frame
            memcpy(nonce, zframe_data(nonce_frame), crypto_secretbox_NONCEBYTES);
            zframe_destroy(&nonce_frame);
            int num_frames = (int) zmsg_size(body) - 1;
            int i = 0;
            // zsys_debug("BROKER: Decrypting with key %2x %2x ... %2x %2x ", key[0], key[1],key[crypto_kx_SESSIONKEYBYTES - 2], key[crypto_kx_SESSIONKEYBYTES - 1]);
            int errror_count = 0;
            zframe_t *data_frame;
            for (i = 0; i < num_frames; i++) {
                data_frame = zmsg_pop(body);
                if (data_frame) {
                    unsigned char *ciphertext = zframe_data(data_frame);
                    if (ciphertext) {
                        size_t ciphertextlen = zframe_size(data_frame);
                        size_t plaintextlen = ciphertextlen - crypto_secretbox_MACBYTES;
                        unsigned char *plaintext = (unsigned char *) zmalloc(plaintextlen);
                        if (plaintext) {
                            if (0 == crypto_secretbox_open_easy(plaintext, ciphertext,
                                                                (unsigned long long int) ciphertextlen, nonce,
                                                                key)) {
                                zmsg_addmem(body, plaintext, plaintextlen);
                                free(plaintext);

                                // zsys_debug("BROKER: decrypted data frame #%d", (i + 1));
                                // increment the nonce for the next frame (if any)
                                sodium_increment(nonce, crypto_secretbox_NONCEBYTES);
                            } else {
                                errror_count++;
                            }
                        }
                    }
                    zframe_destroy(&data_frame);
                }
            }
            if (0 == errror_count) {
                // get/decrypt "Canary" frame
                data_frame = zmsg_pop(body);
                size_t ciphertextlen = zframe_size(data_frame);
                size_t plaintextlen = ciphertextlen - crypto_secretbox_MACBYTES;
                unsigned char *plaintext = (unsigned char *) zmalloc(plaintextlen);
                int res = crypto_secretbox_open_easy(plaintext, zframe_data(data_frame),
                                                     (unsigned long long int) ciphertextlen, nonce,
                                                     key);
                zframe_destroy(&data_frame);
                if (0 == res &&
                    0 == memcmp(plaintext, "BB_MDP_SECURE", strlen("BB_MDP_SECURE"))) {
                    rc = 0;
                } else {
                    zsys_error("BROKER: Decryption error - Check failed");
                    zmsg_destroy(&body);

                }
                free(plaintext);
            } else {
                zsys_error("BROKER: Failed to decrypt at least one data frame");
            }
        } else {
            zsys_error("BROKER: malformed body: no data frame");
        }
    }
    return rc;
}

static int s_broker_decrpt_worker_msg(client_t *self, worker_t *worker) {
    int rc = -1;
    zmsg_t *body = mdp_msg_body(self->message);
    if (body) {
        // do we need to decrypt first?
        zframe_t *encryption_indicator_frame = zmsg_pop(body);
        if (encryption_indicator_frame) {
            if (zframe_streq(encryption_indicator_frame, "BB_MDP_SECURE")) {
                rc = s_broker_decrypt_body_frames(body, worker->session_key_rx);
            } else if (zframe_streq(encryption_indicator_frame, "BB_MDP_PLAIN")) {
                // zsys_debug("Plain worker message");
                rc = 0;
            } else {
                zsys_error("Malformed reply: no encryption indicator encryption_indicator_frame");
            }
            zframe_destroy(&encryption_indicator_frame);
        } else {
            zsys_error("Malformed reply: no body");
        }
    }
    return rc;
}

static int s_broker_decrypt_client_msg(client_t *self) {
    int rc = -1;
    zmsg_t *body = mdp_msg_body(self->message);
    if (body) {
        // is it encrypted?
        zframe_t *f = zmsg_pop(body); // get the first body frame = encryption indicator
        if (NULL != f) {
            if (zframe_streq(f, "BB_MDP_SECURE")) {
                // zsys_debug("BROKER: client request is ENCRYPTED");
                zframe_t *client_key = zmsg_pop(body); //  client pubkey frame
                if (client_key) {
                    if (self->client_pk)
                        free(self->client_pk);

                    if (self->session_key_tx)
                        free(self->session_key_tx);

                    if (self->session_key_rx)
                        free(self->session_key_rx);


                    self->client_pk = (unsigned char *) zmalloc(crypto_kx_PUBLICKEYBYTES);
                    self->session_key_tx = (unsigned char *) zmalloc(crypto_kx_SESSIONKEYBYTES);
                    self->session_key_rx = (unsigned char *) zmalloc(crypto_kx_SESSIONKEYBYTES);
                    if (self->client_pk && self->session_key_tx && self->session_key_rx) {
                        // Store the ephemeral public key TODO: Why store it?
                        memcpy(self->client_pk, zframe_data(client_key), crypto_kx_PUBLICKEYBYTES);

                        // calculate the session keys
                        if (0 == crypto_kx_server_session_keys(self->session_key_rx, self->session_key_tx,
                                                               self->server->my_pk,
                                                               self->server->my_sk, self->client_pk)) {
                            // decrypt the body frames
                            rc = s_broker_decrypt_body_frames(body, self->session_key_rx);
                        } else {
                            zsys_error("Failed to generate session keys");
                        }
                    } else {
                        zsys_error("Memory allocation failed for session keys");
                    }
                    zframe_destroy(&client_key); // Client pubkey frame
                }
            } else if (zframe_streq(f, "BB_MDP_PLAIN")) {
                // zsys_debug("BROKER: client request is PLAIN");
                rc = 0;
            } else {
                zsys_error("Invalid request - missing encryption indicator");
                rc = -1;
            }
            zframe_destroy(&f); // Encryption indicator frame
        }
    }
    return rc;
}


//  ---------------------------------------------------------------------------
//  handle_request
//

static void
handle_request(client_t *self) {

    if (0 == s_broker_decrypt_client_msg(self)) {
        const char *service_name = mdp_msg_service(self->message);

        if (strstr(service_name, "mmi.")) {
            handle_mmi(self, service_name);
            return;
        }

        // Create a fresh instance of mdp_msg_t to append to the list of requests.
        mdp_msg_t *msg = mdp_msg_new();

        // routing id, messageid, service, body
        mdp_msg_set_routing_id(msg, mdp_msg_routing_id(self->message));
        mdp_msg_set_id(msg, mdp_msg_id(self->message));
        mdp_msg_set_service(msg, service_name);
        zmsg_t *body = mdp_msg_get_body(self->message);
        mdp_msg_set_body(msg, &body);
        service_t *service = s_service_require(self->server, service_name);
        zlist_append(service->requests, msg);
        s_service_dispatch(service);
    } else {
        zsys_error("Malformed request");
    }
}


//  ---------------------------------------------------------------------------
//  handle_worker_partial
//

static void
handle_worker_partial(client_t *self) {
    //identify the worker -> needed for decryption
    worker_t *worker = s_get_worker_by_routing_ID(self->server->workers, mdp_msg_routing_id(self->message));
    if (worker) {
        if (0 == s_broker_decrpt_worker_msg(self, worker)) {
            mdp_msg_t *worker_msg = self->message;
            mdp_msg_t *client_msg = mdp_msg_new();
            // Set routing id, messageid, service, body
            zframe_t *address = mdp_msg_address(worker_msg);

            mdp_msg_set_routing_id(client_msg, address);
            mdp_msg_set_id(client_msg, MDP_MSG_CLIENT_PARTIAL);
            mdp_msg_set_service(client_msg, mdp_msg_service(worker_msg));
            zmsg_t *body = mdp_msg_get_body(worker_msg);
            mdp_msg_set_body(client_msg, &body);
            // encrypt the body with client keys before sending
            s_broker_encrypt_client_msg(self, client_msg);

            mdp_msg_send(client_msg, self->server->router);
            mdp_msg_destroy(&client_msg);
        }
    } else {
        zsys_error("Could not identify sending worker - decryption failed");
    }
}


//  ---------------------------------------------------------------------------
//  handle_worker_final
//

static void
handle_worker_final(client_t *self) {
    //identify the worker -> needed for decryption
    worker_t *worker = s_get_worker_by_routing_ID(self->server->workers, mdp_msg_routing_id(self->message));
    if (worker) {
        if (0 == s_broker_decrpt_worker_msg(self, worker)) {
            mdp_msg_t *worker_msg = self->message;
            mdp_msg_t *client_msg = mdp_msg_new();
            // Set routing id, messageid, service, body
            zframe_t *address = mdp_msg_address(worker_msg);

            mdp_msg_set_routing_id(client_msg, address);
            mdp_msg_set_id(client_msg, MDP_MSG_CLIENT_FINAL);
            mdp_msg_set_service(client_msg, mdp_msg_service(worker_msg));
            zmsg_t *body = mdp_msg_get_body(worker_msg);
            mdp_msg_set_body(client_msg, &body);
            // encrypt the body with client keys before sending
            s_broker_encrypt_client_msg(self, client_msg);

            mdp_msg_send(client_msg, self->server->router);
            mdp_msg_destroy(&client_msg);


            // Add the worker back to the list of waiting workers.
            zlist_append(self->server->waiting, worker);
            service_t *service = (service_t *) zhash_lookup(self->server->services,
                                                            worker->service->name);
            assert(service);
            zlist_append(service->waiting, worker);

        }
    } else {
        zsys_error("Could not identify sending worker - decryption failed");
    }
}

//  ---------------------------------------------------------------------------
//  handle_disconnect
//

static void
handle_worker_disconnect(worker_t *self) {
    delete_worker(self);
}


//  ---------------------------------------------------------------------------
//  destroy_broker
//

static void
destroy_broker(client_t *self) {

}


//  ---------------------------------------------------------------------------
//  handle_ready
//

static void
handle_ready(client_t *self) {
    mdp_msg_t *msg = self->message;
    const char *service_name = mdp_msg_service(msg);
    zframe_t *routing_id = mdp_msg_routing_id(msg);
    if (routing_id) {
        char *identity = zframe_strhex(routing_id);
        zsys_debug("handle_ready: worker %s reports READY for service=\"%s\"", identity, service_name);
        fprintf(stdout, "handle_ready: worker %s reports READY for service=\"%s\"\r\n", identity, service_name);

        int worker_ready = (zhash_lookup(self->server->workers, identity) != NULL);
        free(identity);

        worker_t *worker = s_worker_require(self->server, routing_id);
        if (worker_ready) // Not first command in session.
        {
            s_worker_delete(worker, 1);
        } else {
            // store reference to the worker in the client struct
            self->worker = worker;

            // Check if we need to perform the key exchange
            zmsg_t *ready_body = mdp_msg_get_body(msg);
            if (ready_body) {
                zframe_t *f = zmsg_pop(ready_body); // empty frame
                if (f)
                    zframe_destroy(&f);
                f = zmsg_pop(ready_body); // authkey?
                if (f) {
                    char *authkey = zframe_strdup(f);
                    if (self->server->known_psks) {
                        if (authkey) {
                            //zsys_debug("Got worker Authkey: %s", authkey);
                            //check if we know this worker's authkey
                            int res = zlist_exists(self->server->known_psks, authkey);
                            free(authkey);
                            if (0 == res) {
                                zsys_warning("Worker authenticated with an unknown key");
                                s_worker_delete(worker, 1);
                                //zmsg_destroy(&ready_body);
                                return;
                            }
                            zframe_destroy(&f);
                            f = zmsg_pop(ready_body);
                            if (f) {
                                //zsys_debug("Got worker KX PK frame : %s", zframe_strhex(f));
                                unsigned char *worker_kx_pk = (unsigned char *) zframe_strdup(f);
                                zframe_destroy(&f);
                                if (worker_kx_pk) {
                                    if (self->server->my_sk && self->server->my_pk) {
                                        // generate keys
                                        worker->session_key_tx = (unsigned char *) zmalloc(crypto_kx_SESSIONKEYBYTES);
                                        worker->session_key_rx = (unsigned char *) zmalloc(crypto_kx_SESSIONKEYBYTES);

                                        if (crypto_kx_server_session_keys(worker->session_key_rx,
                                                                          worker->session_key_tx,
                                                                          self->server->my_pk, self->server->my_sk,
                                                                          worker_kx_pk) !=
                                            0) {
                                            zsys_error("Failed to create session keys");
                                            s_worker_delete(worker, 1);
                                            free(worker_kx_pk);
                                            return;
                                        }
                                    }
                                    free(worker_kx_pk);
                                }
                            }
                        }
                    } else {
                        zsys_error("No known_worker keys loaded");
                    }
                }
                zmsg_destroy(&ready_body);
            }
            service_t *service = s_service_require(self->server, service_name);
            worker->service = service;
            zlist_append(service->broker->waiting, worker);
            zlist_append(service->waiting, worker);
            worker->service->workers++;
            s_service_dispatch(service);
        }
    }
}


//  ---------------------------------------------------------------------------
//  reset_timeouts
//

static void
reset_timeouts(client_t *self) {
    self->timeouts = 0;
}


//  ---------------------------------------------------------------------------
//  handle_set_wakeup
//

static void
handle_set_wakeup(client_t *self) {
    // sending in 10% shorter intervals than we're checking
    engine_set_wakeup_event(self, HEARTBEAT_DELAY, send_heartbeat_event);
}


//  ---------------------------------------------------------------------------
//  delete_worker
//

static void
delete_worker(worker_t *self) {
    if (self != NULL)
        s_worker_delete(self, 0);
}


//  ---------------------------------------------------------------------------
//  check_timeouts
//

static int
check_timeouts(client_t *self) {
    self->timeouts++;
    if (self->timeouts > MAX_TIMEOUTS) {
        engine_set_exception(self, terminate_event);
        return -1;
    }
    return (int) self->timeouts;
}
