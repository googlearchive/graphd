package(default_visibility = ["//visibility:public"])

cc_binary(
    name = "gld",
    srcs = [
        "gld.c",
        "gld.h",
        "gld-primitive.c",
        "gld-request.c",
        "gld-var.c",
    ],
    copts = [
        "$(STACK_FRAME_UNLIMITED)",
        "-g",
        "-w",
    ],
    deps = [
        "//libcl",
        "//libcm",
        "//libgraph",
        "//libgraphdb",
    ],
)
