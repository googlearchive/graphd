# Description:
#   libgraphdb - a C graph repository client library

package(
    default_visibility = ["//visibility:public"],
)

cc_library(
    name = "libgraphdb",
    srcs = [
        "graphdb-address.c",
        "graphdb-args.c",
        "graphdb-buffer-alloc.c",
        "graphdb-buffer-dup.c",
        "graphdb-buffer-format.c",
        "graphdb-buffer-free.c",
        "graphdb-connect.c",
        "graphdb-connection-drop.c",
        "graphdb-create.c",
        "graphdb-descriptor.c",
        "graphdb-destroy.c",
        "graphdb-heap.c",
        "graphdb-initialize.c",
        "graphdb-iterator.c",
        "graphdb-log.c",
        "graphdb-query.c",
        "graphdb-reconnect-async.c",
        "graphdb-request-alloc.c",
        "graphdb-request-cancel.c",
        "graphdb-request-chain.c",
        "graphdb-request-free.c",
        "graphdb-request-io.c",
        "graphdb-request-lookup.c",
        "graphdb-request-send.c",
        "graphdb-request-wait.c",
        "graphdb-server-error.c",
        "graphdb-server-name.c",
        "graphdb-set-logging.c",
        "graphdb-set-loglevel.c",
        "graphdb-set-memory.c",
        "graphdb-set-reply-callback.c",
        "graphdb-strerror.c",
        "graphdb-time.c",
        "graphdb-token.c",
        "graphdb-transaction-id.c",
        "graphdbp.h",
    ],
    hdrs = [
        "graphdb.h",
        "graphdbp.h",
    ],
    copts = ["-g"],
    deps = [
        "//libcl",
        "//libcm",
        "//libgraph",
    ],
)

cc_binary(
  name = "graphdb-to-dot",
  srcs = [
      "graphdb-to-dot.c",
  ],
  deps = [
    "//libcl",
    "//libcm",
    "//libgraph",
    "//libgraphdb",
  ],
)
