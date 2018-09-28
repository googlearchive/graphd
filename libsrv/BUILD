# Description:
#  libsrv - protocol server

package(
    default_visibility = ["//visibility:public"],
)

cc_library(
    name = "libsrv",
    srcs = [
        "srv-address.c",
        "srv-buffer.c",
        "srv-buffer-pool.c",
        "srv-buffered-connection.c",
        "srv-build-version.c",
        "srv-config.c",
        "srv-delay.c",
        "srv-epitaph.c",
        "srv-idle.c",
        "srv-interface.c",
        "srv-interface-socket.c",
        "srv-interface-tcp.c",
        "srv-interface-tty.c",
        "srv-interface-unix.c",
        "srv-main.c",
        "srv-memory-list.c",
        "srv-msclock.c",
        "srv-pidfile.c",
        "srv-request.c",
        "srv-session.c",
        "srv-settle.c",
        "srv-shared.c",
        "srv-sleep.c",
        "srv-stat.c",
        "srv-timeout.c",
        "srv-unixid.c",
        "srvp.h",
    ],
    hdrs = [
        "srv.h",
        "srv-interface.h",
        "srv-interface-socket.h",
        "srv-interface-tcp.h",
    ],
    copts = [
        "$(STACK_FRAME_UNLIMITED)",
        "-w",  # Suppress all warnings
    ],
    #includes = [
    #    "../libcl",
    #    "../libcm",
    #    "../libes",
    #],
    deps = [
        "//libcl",
        "//libcm",
        "//libes",
    ],
)
