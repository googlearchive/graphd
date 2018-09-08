# Description:
#   libgraph - graph repository basic data types

package(
    default_visibility = ["//visibility:public"],
)

cc_library(
    name = "libgraph",
    srcs = [
        "graph-build-version.c",
        "graph-create.c",
        "graph-datatype.c",
        "graph-dateline.c",
        "graph-destroy.c",
        "graph-fuzzy-compare.c",
        "graph-grmap.c",
        "graph-grmap-dateline.c",
        "graph-grmap-equal.c",
        "graph-grmap-next.c",
        "graph-grmap-range.c",
        "graph-grmap-read.c",
        "graph-grmap-table.c",
        "graph-grmap-write.c",
        "graph-guid.c",
        "graph-hullset.c",
        "graph-idset-tile.c",
        "graph-strerror.c",
        "graph-timestamp.c",
        "graphp.h",
    ],
    hdrs = ["graph.h"],
    copts = [
        "-g",
        "-Wno-tautological-compare",
    ],
    deps = [
        "//libcl",
        "//libcm",
    ],
)
