package(default_visibility = ["//visibility:public"])

cc_binary(
    name = "gpush",
    srcs = [
        "gpush.c",
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
