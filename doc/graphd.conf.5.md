# graphd.conf(5) -- graph repository configuration file

## SYNOPSIS

**graphd -f** graph.conf

## DESCRIPTION

The graph repository server, graphd, by default attempts to read a configuration
file /usr/local/etc/graph.conf, if it exists (it is not an error if the file
doesn't exist; you can run graphd without a configuration file).

## DATABASE

    database {
          type <addb>
          path </foo/bar>
          sync <boolean>
          transactional <boolean>
          snapshot </foo/baz>
          istore-init-map-tiles <integer>
          gmap-init-map-tiles <integer>
          id <dbid>
      }

The **type** must currently be either omitted or "addb", the only kind of
database supported. The **path** is the path of the directory where the database
is stored. If the directory doesn't exist at the time graphd starts, it will be
created.

The **id** will be encoded in the database file and affects the GUIDs created by
the server. At any one time, only one graphd in a mutually accessible network of
servers must create GUIDs based on one ID. IDs can either be numbers or short
strings; the strings are case- insensitive and can be at most 7 characters in
length. If no id is supplied, graphd invents one based on the IP address and
process ID of the server. Once a stored databse has an ID, that ID overrides any
IDs specified to the server at start-up.

If **sync** is set to "true" (the default) a blocking fsync is done prior to
returning results from a write operation.

If **transactional** is set to "true" (the default) modifications to the
database and indexes are done in transactional chunks. Setting **transactional**
to false casues graphd to modify database files directly. Direct access is
faster, required for SMP operation. If graphd crashes during a write, the
database will be corrupted and graphd will need to have a separate backup copy
of the database (a snapshot) in order to restart.

If **transactional** is set to "false", **snapshot** gives the location of a
snapshot directory. The snapshot directory must be in the same filesystem as the
database specified by **path**, as graphd will use rename to move the snapshot
to the database path when restarting after a crash which left the database
corrupt.

Setting **{istore,gmap}-init-map-tiles** controls how many tiles are in the
permanently mmap'd in an istore or gmap partition. The default is 32768 tiles
(1GB with 32k tiles) which is intended to be "the whole file" Obviously this
doesn't work if you don't have a 64-bit address space to play with.

Setting hmap-percent controls the percentage of available memory to be devoted
to the hmap. If a new hmap is to be created, it will be sized such that the
index portion of the table consumes the given amount of memory.

## RUNTIME

*   **listen** <u>type</u> **{** <u>parameters</u> **}**:

    Wait for incoming client connections on the specified interface. The
    parameters differ by type:

        listen tcp {
            host hostname
            port portnumber
         }

    For an interface of type "tcp", a host can be specified as either an IP
    address ("127.0.0.1") or a DNS name (which will be resolved in a synchronous
    call to gethostbyname() when the server starts up.)

    The <u>port</u> can be specified etiher as a number, or as a name that will
    be resolved against /etc/services when the server starts up.

*   **replica {** <u>parameters</u> **}**:

*   **archive {** <u>parameters</u> **}**:

*   **import {** <u>parameters</u> **}**:

    Replicate from the server at the given host/port. The "replica" form creates
    a writable replica (write requests are forwarded to the destination server);
    the "archive" form creates a read-only ("archival") copy of the destination.
    The host/port parameters are the same as for "listen tcp":

        replica
        {
             host hostname
             port portnumber
        }
        archive
        {
             host hostname
             port portnumber
        }

*   **pid-file** <u>pathname</u>:

    Store the server's process id in file pathname rather than in the default,
    /var/run/graphd.pid.

*   **user** <u>username</u>:

    After opening the server socket, attempt to change identity to username.

*   **group** <u>groupname</u>:

    After opening the server socket, attempt to change identity to groupname.

*   **core** <u>boolean</u>:

    Set to "true" if core files are desired. If false graphd will attempt to put
    a stacktrace into the log.

*   **cpu** <u>number</u>:

    The cpu id of the cpu to bind to. CPU ids are positive numbers starting
    with 0. 0 is the default.

*   **{short,long}-timeslice-ms** <u>number</u>:

    Specify the number of milliseconds in short and long timeslices. Each
    request receives an initial long timeslice followed by as many short
    timeslices as it takes to finish the request. The default long timeslice is
    100ms, short, 10ms. If interactive response is not required, both may be set
    much longer, say 500ms, for a gain in efficiency.

*   **processes** <u>number</u>:

    The number of graphd listeners to spawn. When this is greater than 1,
    another process, a leader, is spawned to take writes and syncronize the
    following listeners. Requires that transactional be set to false. 1 is the
    default.

*   **smp** <u>boolean</u>:

    A shortcut for processes, above. true sets processes to be the number of
    cores available to the system. false sets processes to 1.

*   **leader-socket** <u>address</u>:

    The socket address which the leader SMP process will listen on. Defaults to
    <u>unix://graphd-smp-socket.PID</u> but will accept TCP sockets as well (eg,
    <u>tcp://localhost:8190</u>)

*   **instance-id** <u>pattern</u>:

    Set the graphd instance ID

## LOGGING

*   **log-level** <u>level</u>:

    Set the application's log-level to level. Available levels (as of March
    2005) are, with increasing importance: <u>spew</u>, <u>enter</u>,
    <u>debug</u>, <u>detail</u>, <u>info</u>, <u>fail</u>, <u>overview</u>,
    <u>error</u>, <u>fatal</u>

    The default level is error.

*   **log-facility** <u>facility</u>:

    When logging via syslog (while running in the background), open the log as
    <u>facility</u>. The default is "user"; other alternatives that might be
    interesting are "local0" through "local7". For an exhaustive list of
    available facilities, see the syslog documentation.

*   **log-ident** <u>ident</u>:

    When logging via syslog (while running in the background), open the log as
    application <u>ident</u>. The default identity is the program name.

*   **log-file** <u>pattern</u>:

    Rather than logging via syslog, append to the file given by an expanded
    strftime(3) evaluation of <u>pattern</u>. In addition to strftime's
    sequences, %$ is replaced with the process ID of the printing process,
    formatted as a decimal integer. The file is created if necessary.

    For instance, if <u>pattern</u> is `graphd.%Y-%m-%dT%H:%MZ.%$', and assuming
    the current time is July 5 2006, 12:10 PM UTC and the server is running as a
    parent and a child process with process IDs 10001 and 10002, then
    files`graphd.2006-07-05T12:10Z.10001' and `graphd.2006-07-05T12:10Z.10002'
    will be opened. For every minute that the child server runs, a new file will
    be created; the parent server is mostly quiescent and will probably not log
    anything other than at startup and shutdown.

    The <u>pattern</u> string is evaluated once every minute and whenever the
    process ID of the evaluating code changes, causing new log files to be
    created automatically if necessary. Notice that seconds specifiers such as
    %S can be included in <u>pattern</u>, but they will only be evaluated once
    every minute.

*   **log-flush** <u>boolean</u>:

    After each write to the logfile, flush the logfile to disk (true) or do no
    such thing (false). By default, the file is flushed to disk after each
    write.

*   **netlog-file** <u>pattern</u>:

    Append a netlog-style transaction log to the file given by the strftime(3)
    evaluation of <u>pattern</u>. For more information, please refer to the
    `log-file' option.

*   **netlog-flush** <u>boolean</u>:

    After each write to the netlog file, flush the file to disk (true) or do no
    such thing (false). By default, the netlog file is **not** flushed to disk
    after each write.

*   **netlog-level** <u>level</u>:

    Set the application's log-level for logging via netlog to <u>level</u>. The
    levels are the same as for <u>log-level</u>. By default, this is set to
    <u>detail</u>, which should be the most useful level for transaction
    logging; note that a netlog-file statement is still needed to turn
    netlogging on to begin with.

## PAGE POOL

The server uses a fixed-size pool of buffers to transfer data between interface
and underlying applications. The initial and maximum sizes of the pool can be
configured, as can the page size per individual buffer.

*   **pool-min** <u>number</u>:

    The minimum number of bytes in the buffer pool. If the number of available
    bytes falls below this limit, only allocations needed to calm down the
    system (e.g. to write results and finish requests) are served.

*   **pool-max** <u>number</u>:

    The maximum number of bytes in the buffer pool. If more than this much
    memory becomes available, pages that become free are actually free'ed, i.e.
    returned to the runtime library.

*   **pool-page-size** <u>bytes</u>:

    Number of bytes per page in the pool.

All numbers in the pool configuration can be suffixed with k, m, g, or t,
multiplying the number before the prefix with 2^10, 2^20, 2^30, or 2^40,
respectively.

## COST

The cost configuration caps the runtime (tr), system time (ts), user time (tu),
page faults (pf), page reclaims (pr), data writes (dw), data reads (dr), index
writes (iw), index reads (ir), index counts (in), and value allocations (va) for
any request executed by the system, including dump and restore requests. Any of
the individual parameters can be omitted.

**cost "tr=# ts=# tu=# pf=# pr=# dw=# dr=# iw=# ir=# in=# va=#"**

This is the short form, similar to the way a cost would be announced in a graphd
request.

    cost {
           tr milliseconds
           ts milliseconds
           tu milliseconds
           pf number
           pr number
           dw number
           dr number
           iw number
           ir number
           in number
           va number
     }

The long form of the cost configuration uses { } and individual name/value pairs
rather than the short form above. Here, to, individual cost parameters can be
omitted.

Regardless of how a cost is specified - be it in the request itself or in the
configuration file - its enforcement is always advisory. It gives the server
leverage to terminate requests that run for too long, but there's no guarantee
that it will.

## FILES

       /usr/local/etc/graph.conf - default configuration file

## SEE ALSO

[graphd(8)](graphd.8.md)
