#ifndef PLASMA_CLIENT_H
#define PLASMA_CLIENT_H

#include <time.h>
#include "plasma.h"

typedef struct plasma_connection plasma_connection;

/**
 * This is used by the Plasma Client to send a request to the Plasma Store or
 * the Plasma Manager.
 *
 * @param conn The file descriptor to use to send the request.
 * @param type The type of request.
 * @param req The address of the request to send.
 * @return Void.
 */
void plasma_send_request(int fd, int type, plasma_request *req);

/**
 * Create a plasma request to be sent with a single object ID.
 *
 * @param object_id The object ID to include in the request.
 * @return The plasma request.
 */
plasma_request make_plasma_request(object_id object_id);

/**
 * Create a plasma request to be sent with multiple object ID. Caller must free
 * the returned plasma request pointer.
 *
 * @param num_object_ids The number of object IDs to include in the request.
 * @param object_ids The array of object IDs to include in the request. It must
 *        have length at least equal to num_object_ids.
 * @return A pointer to the newly created plasma request.
 */
plasma_request *make_plasma_multiple_request(int num_object_ids,
                                             object_id object_ids[]);

/**
 * Try to connect to the socket several times. If unsuccessful, fail.
 *
 * @param socket_name Name of the Unix domain socket to connect to.
 * @param num_retries Number of retries.
 * @param timeout Timeout in milliseconds.
 * @return File descriptor of the socket.
 */
int socket_connect_retry(const char *socket_name,
                         int num_retries,
                         int64_t timeout);

/**
 * Connect to the local plasma store and plasma manager. Return
 * the resulting connection.
 *
 * @param store_socket_name The name of the UNIX domain socket to use to
 *        connect to the Plasma store.
 * @param manager_socket_name The name of the UNIX domain socket to use to
 *        connect to the local Plasma manager. If this is NULL, then this
 *        function will not connect to a manager.
 * @return The object containing the connection state.
 */
plasma_connection *plasma_connect(const char *store_socket_name,
                                  const char *manager_socket_name);

/**
 * Disconnect from the local plasma instance, including the local store and
 * manager.
 *
 * @param conn The connection to the local plasma store and plasma manager.
 * @return Void.
 */
void plasma_disconnect(plasma_connection *conn);

/**
 * Try to connect to a possibly remote Plasma Manager.
 *
 * @param addr The IP address of the Plasma Manager to connect to.
 * @param port The port of the Plasma Manager to connect to.
 * @return The file descriptor to use to send messages to the
 *         Plasma Manager. If connection was unsuccessful, this
 *         value is -1.
 */
int plasma_manager_connect(const char *addr, int port);

/**
 * Create an object in the Plasma Store. Any metadata for this object must be
 * be passed in when the object is created.
 *
 * @param conn The object containing the connection state.
 * @param object_id The ID to use for the newly created object.
 * @param size The size in bytes of the space to be allocated for this object's
          data (this does not include space used for metadata).
 * @param metadata The object's metadata. If there is no metadata, this pointer
          should be NULL.
 * @param metadata_size The size in bytes of the metadata. If there is no
          metadata, this should be 0.
 * @param data The address of the newly created object will be written here.
 * @return Void.
 */
void plasma_create(plasma_connection *conn,
                   object_id object_id,
                   int64_t size,
                   uint8_t *metadata,
                   int64_t metadata_size,
                   uint8_t **data);

/**
 * Get an object from the Plasma Store. This function will block until the
 * object has been created and sealed in the Plasma Store.
 *
 * @param conn The object containing the connection state.
 * @param object_id The ID of the object to get.
 * @param size The size in bytes of the retrieved object will be written at this
          address.
 * @param data The address of the object will be written at this address.
 * @param metadata_size The size in bytes of the object's metadata will be
 *        written at this address.
 * @param metadata The address of the object's metadata will be written at this
 *        address.
 * @return Void.
 */
void plasma_get(plasma_connection *conn,
                object_id object_id,
                int64_t *size,
                uint8_t **data,
                int64_t *metadata_size,
                uint8_t **metadata);

/**
 * Tell Plasma that the client no longer needs the object. This should be called
 * after plasma_get when the client is done with the object. After this call,
 * the address returned by plasma_get is no longer valid. This should be called
 * once for each call to plasma_get (with the same object ID).
 *
 * @param conn The object containing the connection state.
 * @param object_id The ID of the object that is no longer needed.
 * @return Void.
 */
void plasma_release(plasma_connection *conn, object_id object_id);

/**
 * Check if the object store contains a particular object and the object has
 * been sealed. The result will be stored in has_object.
 *
 * @todo: We may want to indicate if the object has been created but not sealed.
 *
 * @param conn The object containing the connection state.
 * @param object_id The ID of the object whose presence we are checking.
 * @param has_object The function will write 1 at this address if the object is
 *        present and 0 if it is not present.
 * @return Void.
 */
void plasma_contains(plasma_connection *conn,
                     object_id object_id,
                     int *has_object);

/**
 * Seal an object in the object store. The object will be immutable after this
 * call.
 *
 * @param conn The object containing the connection state.
 * @param object_id The ID of the object to seal.
 * @return Void.
 */
void plasma_seal(plasma_connection *conn, object_id object_id);

/**
 * Delete an object from the object store. This currently assumes that the
 * object is present and has been sealed.
 *
 * @todo We may want to allow the deletion of objects that are not present or
 *       haven't been sealed.
 *
 * @param conn The object containing the connection state.
 * @param object_id The ID of the object to delete.
 * @return Void.
 */
void plasma_delete(plasma_connection *conn, object_id object_id);

/**
 * Fetch objects from remote plasma stores that have the
 * objects stored.
 *
 * @param manager A file descriptor for the socket connection
 *        to the local manager.
 * @param object_id_count The number of object IDs requested.
 * @param object_ids[] The vector of object IDs requested. Length must be at
 *        least num_object_ids.
 * @param is_fetched[] The vector in which to return the success
 *        of each object's fetch operation, in the same order as
 *        object_ids. Length must be at least num_object_ids.
 * @return Void.
 */
void plasma_fetch(plasma_connection *conn,
                  int num_object_ids,
                  object_id object_ids[],
                  int is_fetched[]);

/**
 * Wait for objects to be created (right now, wait for local objects).
 *
 * @param conn The object containing the connection state.
 * @param num_object_ids Number of object IDs wait is called on.
 * @param object_ids Object IDs wait is called on.
 * @param timeout Wait will time out and return after this number of ms.
 * @param num_returns Number of object IDs wait will return if it doesn't time
 *        out.
 * @param return_object_ids Out parameter for the object IDs returned by wait.
 *        This is an array of size num_returns. If the number of objects that
 *        are ready when we time out, the objects will be stored in the last
 *        slots of the array and the number of objects is returned.
 * @return Number of objects that are actually ready.
 */
int plasma_wait(plasma_connection *conn,
                int num_object_ids,
                object_id object_ids[],
                uint64_t timeout,
                int num_returns,
                object_id return_object_ids[]);

/**
 * Subscribe to notifications when objects are sealed in the object store.
 * Whenever an object is sealed, a message will be written to the client socket
 * that is returned by this method.
 *
 * @param conn The object containing the connection state.
 * @return The file descriptor that the client should use to read notifications
           from the object store about sealed objects.
 */
int plasma_subscribe(plasma_connection *conn);

/**
 * Get the file descriptor for the socket connection to the plasma manager.
 *
 * @param conn The plasma connection.
 * @return The file descriptor for the manager connection. If there is no
 *         connection to the manager, this is -1.
 */
int get_manager_fd(plasma_connection *conn);


/* === alternate API === */

/* Object is stored on the local Plasma Store. */
#define PLASMA_OBJECT_LOCAL  1
/* Object is not stored on the local Plasma Store */
#define PLASMA_OBJECT_NOT_LOCAL 2
/* Object is stored on a remote Plasma store, and it is not stored on the local Plasma Store */
#define PLASMA_OBJECT_REMOTE 3
/* Object is not stored in the system. */
#define PLASMA_OBJECT_DOES_NOT_EXIST 5
/* Object is currently transferred from a remote Plasma store the the local Plasma Store. */
#define PLASMA_OBJECT_IN_TRANSFER 6
#define PLASMA_OBJECT_ANYWHWERE 7

enum plasma_client_notification_type {
  /** Object has become available on the local Plasma Store. */
          PLASMA_OBJECT_READY_LOCAL = 256,
  /** Object has become available on a remote Plasma Store. */
          PLASMA_OBJECT_READY_REMOTE
};



/**
 * Object buffer data structure.
 */
typedef struct {
  /* The size in bytes of the object. */
  int64_t size;
  /* The address of the object data */
  uint8_t *data;
  /* The metadata size in bytes */
  int64_t metadata_size;
  /* The address of the metadata */
  uint8_t *metadata;
} object_buffer;

/**
 * Object information data structure.
 */
typedef struct {
  /** time when the object was created (sealed) */
  time_t last_access_time;
  /** time when the object was accessed the last time */
  time_t creation_date;
  uint64_t refcount;
} object_info;


/**
 * Get specified object from the local Plasma Store. This function is non-blocking.
 *
 * @param conn The object containing the connection state.
 * @param object_id The ID of the object to get.
 * @param objectBuffer The data structure where the object information will be written.
 * @return PLASMA_CLIENT_LOCAL, if the object is stored at the local Plasma Store.
 *         PLASMA_CLIENT_NOT_LOCAL, if not. In this case, the caller needs to
 *         ignore data, metadata_size, and metadata fields.
 */
int plasma_get_local1(plasma_connection *conn,
                      object_id object_id,
                      object_buffer *object_buffer);

/**
 * Initiates the fetch (transfer) of an object from a remote Plasma Store.
 *
 * If object is stored in the local Plasma Store, tell the caller.
 *
 * If not, check whether the object is stored on a remote Plasma Store. In yes, and
 * if either a transfer for the object has been scheduled or the object is in transfer
 * return. Otherwise schedule a transfer for the object.
 *
 * If object is available neither locally nor remotely, the client has to invoke
 * the local scheduler to (re)create the object.
 *
 * This function is non-blocking.
 *
 * @param conn The object containing the connection state.
 * @param object_id The ID of the object to be fetched.
 * @return PLASMA_CLIENT_LOCAL, if the object is stored on the local Plasma Store.
 *         PLASMA_CLIENT_WAIT, if object is not stored on the local Plasma Store but
 *         it is available on a remote Plasma Store.
 *         PLASMA_CLIENT_DOES_NOT_EXIST, if object is available neither locally nor remotely.
 */
int plasma_fetch_remote1(plasma_connection *conn, object_id object_id);

/**
 * Return the status of a given object.
 *
 * @param conn The object containing the connection state.
 * @param object_id The ID of the object whose status we query.
 * @param total_size Object size in bytes.
 * @param transferred_size The number of bytes from object transferred so far.
 *                         This is non zero only when PLASMA_CLIENT_IN_TRANSFER is returned.
 * @return PLASMA_CLIENT_LOCAL, if object is stored in the local Plasma Store.
 *         has been already scheduled by the Plasma Manager.
 *         PLASMA_CLIENT_IN_TRANSFER, if the object is currently being scheduled for
 *                                    being transferred or it is transferring.
 *         In this case, transferred_bytes represent the number of bytes transferred so far.
 *         PLASMA_CLIENT_REMOTE, if the object is stored at a remote Plasma Store.
 *         PLASMA_CLIENT_DOES_NOT_EXIST, if the object doesn’t exist in system.
 */
int plasma_status1(plasma_connection *conn,
                   object_id object_id,
                   int64_t *total_size,
                   int64_t *transferred_size);


/**
 * Return the information associated to a given object.
 *
 * @param conn The object containing the connection state.
 * @param object_id The ID of the object whose info the client queries.
 * @param object_info The object's infirmation.
 * @return PLASMA_CLIENT_LOCAL, if the object is stored in the local Plasma Store.
 *         PLASMA_CLIENT_NOT_LOCAL, if not. In this case, the caller needs to
 *         ignore data, metadata_size, and metadata fields.
 */
int plasma_info(plasma_connection *conn,
                object_id object_id,
                object_info *object_info);


/**
 * Object rquest data structure. Used in the plasma_wait_for_objects() argument.
 */
typedef struct {
  /** ID of the requested object. If ID_NIL request any object */
  object_id object_id;
  /** Request associated to the object. It can take one of the following values:
   * - PLASMA_OBJECT_LOCAL: return if or when the object is available in the local Plasma Store.
   * - PLASMA_OBJECT_ANYWHWERE: return if or when the object is available in the
   *                            system (i.e., either in the local or a remote Plasma Store. */
  int type;
  /** Object status. Same as the status returned by plasma_status() function call.
   *  This is filled in by plasma_wait_for_objects1():
   * - PLASMA_OBJECT_READY_LOCAL: object is ready at the local Plasma Store.
   * - PLASMA_OBJECT_READY_REMOTE: object is ready at a remote Plasma Store.
   * - PLASMA_OBJECT_DOES_NOT_EXIST: object does not exist in the system.
   * - PLASMA_CLIENT_IN_TRANSFER, if the object is currently being scheduled for
   *                              being transferred or it is transferring. */
  int status;
} object_request;

/**
 * Wait for a bunch of objects to be available (sealed) in the local Plasma Store
 * or in a remote Plasma Store, or for a timeout to expire. This is a blocking call.
 *
 * @param conn The object containing the connection state.
 * @param num_object_requests Size of the object_requests array.
 * @param object_requests Object event array. Each element contains a request for a particular
 *                        object_id. The type of request is specified in the "type" field.
 *                        A PLASMA_OBJECT_LOCAL request is satisfied when object_id becomes
 *                        available in the local Plasma Store. In this case, the "status"
 *                        field is set to PLASMA_OBJECT_LOCAL.
 *                        A PLASMA_OBJECT_ANYWHERE request is satisfied when object_id becomes
 *                        available either at the local Plasma Store or on a remote Plasma Store.
 *                        In this case, the "status" field is set to either PLASMA_OBJECT_LOCAL
 *                        or PLASMA_OBJECT_LOCAL remote.
 *                        If object_id is ID_NIL, the function will return any object with an
 *                        object_id that doesn't match any specified object_id in object_requests
 *                        and which satisfies the request "type".
 * @param min_num_ready_objects The minimum number of requests in object_requests array that must be
 *                              satisfied before the function returns, unless it timeouts.
 *                              min_num_ready_objects should be no larger than num_object_requests.
 * @param timeout_ms  Timeout value in milliseconds. If this timeout expires before
 *                    "min_num_ready_objects" of requests are satisfied, the function returns.
 * @return Number of satisfied requests in the object_requests list. If the returned number is less
 *         than min_num_ready_objects this means that timeout expired.
 */
int plasma_wait_for_objects1(plasma_connection *conn,
                             int num_object_requests,
                             object_request object_requests[],
                             int min_num_ready_objects,
                             int timeout_ms);



#endif /* PLASMA_CLIENT_H */
