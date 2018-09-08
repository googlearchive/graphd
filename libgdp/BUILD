# Description:
#   gdp - graph language parser

package(
    default_visibility = ["//visibility:public"],
)

cc_library(
    name = "libgdp",
    srcs = [
        "gdp.c",
        "gdp-input.c",
        "gdp-lexer.c",
        "gdp-micro.c",
        "gdp-parse.c",
        "gdp-private.h",
        "gdp-token.c",
    ],
    hdrs = [
        "gdp.h",
        "gdp-error.h",
        "gdp-input.h",
        "gdp-lexer.h",
        "gdp-micro.h",
        "gdp-misc.h",
        "gdp-output.h",
        "gdp-token.h",
    ],
    copts = ["-g"],
    deps = [
        "//graphd:graphdh",
        "//libaddb",
        "//libcl",
        "//libcm",
        "//libes",
        "//libgraph",
        "//libpdb",
        "//libsrv",
    ],
)
