# Description:
#   es - This is poll(2) on steroids.

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "libes",
    srcs = [
        "es-application-event.c",
        "es-build-version.c",
        "es-close.c",
        "es-create.c",
        "es-demon.c",
        "es-destroy.c",
        "es-idle.c",
        "es-loop.c",
        "es-now.c",
        "es-open.c",
        "es-set-post-dispatch.c",
        "es-set-pre-dispatch.c",
        "es-subscribe.c",
        "es-timeout.c",
        "es-unsubscribe.c",
        "esp.h",
    ],
    hdrs = ["es.h"],
    copts = ["-g"],
    deps = [
        "//libcl",
        "//libcm",
    ],
)
