/*
Copyright 2018 Google Inc. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef SRV_INTERFACE_TCP_H
#define SRV_INTERFACE_TCP_H 1

#if __FreeBSD__

/* TCP state names differ on FreeBSD, so we're defining the Linux variants with
 * their FreeBSD counterparts.
 */
#include <netinet/tcp_fsm.h>

#define TCP_ESTABLISHED TCPS_ESTABLISHED
#define TCP_SYN_SENT TCPS_SYN_SENT
#define TCP_SYN_RECV TCPS_SYN_RECEIVED
#define TCP_FIN_WAIT1 TCPS_FIN_WAIT_1
#define TCP_FIN_WAIT2 TCPS_FIN_WAIT_2
#define TCP_TIME_WAIT TCPS_TIME_WAIT
#define TCP_CLOSE TCPS_CLOSED
#define TCP_CLOSE_WAIT TCPS_CLOSE_WAIT
#define TCP_LAST_ACK TCPS_LAST_ACK
#define TCP_LISTEN TCPS_LISTEN
#define TCP_CLOSING TCPS_CLOSING

#endif /* __FreeBSD__ */

#endif /* SRV_INTERFACE_TCP_H */
