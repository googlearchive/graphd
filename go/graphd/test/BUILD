load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["fake.go"],
    importpath = "github.com/google/graphd/go/graphd/go/graphd/test",
    visibility = ["//visibility:public"],
)

go_test(
    name = "go_default_test",
    srcs = ["fake_test.go"],
    embed = [":go_default_library"],
)
