tests = glob(["*.sh"])

[
    sh_test(
        name = test[0:-3],
        srcs = ["test.sh"],
        args = [
            test,
        ],
        data = [
            test,
            "rungraphd",
            "//graphd",
        ] + glob([
            "%s*.exp" % test[:-3],
            "%s*.in" % test[:3],
            "*.conf",
        ]),
    )
    for test in tests
]
