# graphd(8) - graph repository server

## SYNOPSIS

**graphd [options...]**

## DESCRIPTION

       graphd is the graph repository server.  It accepts a configuration file
       and command line parameters, and allows connecting clients to  add  and
       query graph primitives.

## GETTING STARTED

       To run a copy of graphd for yourself, editing the database in the local
       directory MYDB, reading and writing to the terminal, use

            ./graphd -y -d MYDB

       To run graphd as a database server  for  yourself  on  localhost,  port
       1234, locking against itself using process ID file tmp/MYPID, run

            ./graphd -d MYDB -i tcp://127.0.0.1:1234 -p tmp/MYPID

       (The latter command will return immediately, but graphd will be running
       as a background demon once it completes.)

## OPTIONS

       -f configfile
              Read   configuration   information   from   configfile.     (See
              graph.conf(5) for the configuration file syntax.)

       -d pathname
              Use pathname as the database directory, rather than the default,
              /db/graphd.

       -r     Run as a replica server.   We're  still  working  on  what  that
              means.

       -p pathname
              Store  the  server's  process  id in pathname rather than in the
              default, /var/run/graphd.pid.

       -y     Interactive mode.  Instead of  running  as  a  server  with  TCP
              interfaces,  read  from  stdin, and write to stdout.  This is an
              easy way of  using  graphd  to  execute  test  scripts,  or  try
              something out with a private database.

       -n     foreground  mode.   Normally,  the server stays in foreground at
              startup until it thinks it can start correctly and is  ready  to
              actually  accept  incoming  calls;  then  it backgrounds itself,
              detaches from its controlling terminal, and changes into its own
              process group.  With the -n flag, the server parent process does
              not background itself, and does not detach from the  controlling
              terminal,  and  does  not  create  its  own  process group.  The
              process continues to exist until the server terminates for good.

       -b     Boring mode.  In order to  produce  repeatable  testresults  and
              IDs, this option turns time- or environment-sensitive mechanisms
              off and replaces them  with  repeatable  zero  results  whenever
              possible.   For  example,  instead  of  the time stamps that are
              normally used  to  generate  GUIDs,  the  "boring"  mode  simply
              increments a counter when a new object is generated.

       -s spec
              Sabotage.  In  sabotage mode, certain parts of the server suffer
              instances of recoverable failure.  (E.g., iterators  pretend  to
              run  out  of budget and return EWOULDBLOCK.)  Its results should
              still be correct, although they may differ from predictions;  it
              should  not  crash  or  exit.  For a given server executable and
              sabotage specification,  results  should  be  repeatable  within
              reason.

       The specification has the format [0x][loglevel:]N[/MAX][+[+]].
              The leading 0x, if present, causes graphd to fill its stack with
              nonsense between coroutinen calls.  (We're trying to ensure that
              local  variables  lose  their value between sequential calls, to
              detect instances of them not being properly saved and restored.
              The loglevel is the level at which the sabotage module logs that
              it  is  attempting  sabotage.   The N specifies the Nth sabotage
              opportunity that is taken.
              The  optional  /MAX  specifies  how  many  times  each  sabotage
              opportunity should be executed before attempts on it stop and it
              no longer counts as a sabotage opportunity.
              One trailing + means that every  Nth  opportunity  for  sabotage
              should  be  taken;  two trailing ++ increment the N between such
              cycles (causing the pattern to vary more strongly, with  growing
              gaps between instances of sabotage.)

       -l pathname
              Instead  of  logging  to  syslog  (the  default),  log to a file
              pathname.  The file is truncated or created at startup.
       -i interface
              Listen   on   interface   rather   than    on    the    default,
              tcp://0.0.0.0:1234.   The Interface syntax is an optional prefix
              tcp: or local:, followed by specific information for the  chosen
              kind of interface.

              tcp://host[:port]  describes  the  interface  number  port on IP
              address host.  (The host can be specified  either  as  a  domain
              name or as an IP address.)

       -t     Use  a tracing allocator.  This will make graphd slightly slower
              (quadratically slower with number of allocated  fragments),  but
              will detect writing memory over- and underruns early.

       -v detail
              Increase  verbosity.   The  argument  to -v is a comma-separated
              list of zero or more  log-parameters.   Loglevels  come  in  two
              flavors:  there's  a  linear  level  component,  with  ascending
              verbosity one of fatal, error,  overview,  fail,  info,  detail,
              debug,  and  spew;  and a bit-wise facility component that turns
              logging on or of for a specific section of the code.  Currently,
              two  facilities  are known: "tile" (for the ADDB tile cache) and
              "query" (for the  graphd  query  optimizer).   ("tile"  is  used
              inside  the  code,  query  isn't).  The ultra-verbose "spew" log
              mode implicitly turns on any logging that is to  be  turned  on.
              So,  for  example,  if  I wanted to listen to the protocol-level
              exchange of requests, punctuated by tile  cache  allocations,  I
              could  invoke  graphd -vdetail,tile.  For compatibility with the
              previous version of -v, "-vv" is accepted  as  a  shorthand  for
              "-vdetail", and "-vvv" is accepted as a shorthand for "-vdebug".

       -e number
              Freeze (and later re-thaw) read operations every number chances.
              This helps test the correctness of code that normally only  gets
              executed  when  long-running  reads  are  interrupted by writes.
              This may slow down graphd considerably.  A value  of  1  freezes
              every  time;  a  value  of  0  (the  default) turns off freezing
              (unless a read command really does get interrupted by a write).

       -c dirpath
              Enable code coverage tracing.  When started with the path  of  a
              code-coverage  directory,  graphd  will  create  and update code
              coverage points within the named directory for  each  cl_cover()
              macro  executed  during its execution.  The code coverage points
              have the form of files whose names are composed of filename:line
              of  the  cl_cover() statements.  Tools like "cocoa" can generate
              human-readable or XML reports, given such a  directory  and  the
              source code of an application.

       -u user
              Run  as  user  user  rather  than  as  the  user who invoked the
              process.  This can be used to have a server start as  root  (and
              open  its interface socket as root), then change identities to a
              less powerful user.

       -g group
              Run as group group rather than as the  group  of  the  user  who
              invoked the process.  The group equivalent to -u.

       -x pathname
              The  executable path is pathname rather than argv[0].  The debug
              version of graphd (which is all that exists at the  moment  [Mar
              2005])  runs  gdb on itself if it crashes.  In order to properly
              start gdb, it needs to know its pathname.  Usually,  executables
              are  invoked  with  their own pathname as argv[0], but there are
              cases, most notably invokations via the first line  of  a  shell
              script,  where  that  isn't  true.   So, for these cases, the -x
              option of graphd explicitly sets the pathname of the executable.

       -h     Help; print a brief command-line quickreference and exit.

       -m     Modules;  print  modification  listings  for  graphd   and   its
              libraries and exit.

       -w     Wersion; print the graphd format version number (an integer) and
              exit.
              Two  graphd  executables  with  the  same  version  number   are
              compatible  (both  forwards  and  backwards).   If  the  version
              numbers are different there is a database file format change and
              data  will  have  to  be  backed  up  by  the old executable and
              restored by the new executable.

       -z     Stop a running server.


       -q     Test whether a server is running.  If graphd -q exits with  Unix
              exit  code  0  (ok),  a graphd is running with the specified pid
              file.  If it exits with Unix exit code 1, no graphd is running.
              Script example:
                   if ! ./graphd -p mypid.pid -q
                   then
                        echo "Starting graphd."
                        ./graphd -p mypid.pid ...options...
                   fi
                or

                   if ./graphd -p mypid.pid -q
                   then
                        echo "Shutting down graphd."
                        ./graphd -p mypid.pid -z
                   else
                        echo "graphd already shut down."
                   fi

## TODO

Finish the documentation.

## FILES

/db/graphd - default database directory (use -d to override)

## SEE ALSO

[graphd.conf(5)](graphd.conf.5.md)
