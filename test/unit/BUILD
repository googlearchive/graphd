[
    sh_test(
        name = test[0:-3],
        srcs = ["test.sh"],
        args = [
            "-v",
            test,
        ],
        data = [
            test,
            "rungraphd",
            "//graphd",
            "//util:gpush",
        ] + glob([
            "%s*.exp" % test[:-3],
            "%s*.in" % test[:3],
            "*.conf",
        ]),
    )
    for test in glob(["*.sh"])
    if test != "test.sh"
]
