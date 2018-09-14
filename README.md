[![Build Status](https://travis-ci.org/google/graphd.svg?branch=master)](https://travis-ci.org/google/graphd)

# Graphd, the Metaweb Graph Repository

For an introduction to graphd, see
[A Brief Introduction to Graphd](doc/a-brief-tour-of-graphd.md)

This is not an official Google product.

## Building

Build with [bazel](https://bazel.build/), like so:

    (¬‿¬) bazel build graphd
    ..............................................
    INFO: Analysed target //graphd:graphd (16 packages loaded).
    INFO: Found 1 target...
    Target //graphd:graphd up-to-date:
      bazel-bin/graphd/graphd
      INFO: Elapsed time: 23.070s, Critical Path: 0.84s
      INFO: Build completed successfully, 377 total actions

## Running graphd

To get started:

    (¬‿¬) bazel-bin/graphd/graphd -y -d /tmp/db
    graphd> write(name = "HAS_KEY")
    ok (d119a8c04000dcb38000000000000000)
    graphd> read()
    ok ((d119a8c04000dcb38000000000000000 null "HAS_KEY" null null null true true 2018-09-07T19:41:57.0000Z))
    graphd>

For more details, see [graphd.conf(5)](doc/graphd.conf.5.md) and
[graphd(8)](doc/graphd.8.md)
