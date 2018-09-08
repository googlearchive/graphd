/*
Copyright 2015 Google Inc. All rights reserved.
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
#include "libcl/cl.h"
#include "srvp.h"

static void stat_chain(srv_session const* chain, char const* title) {
  srv_session const* ses;

  if ((ses = chain) != NULL) {
    cl_log(ses->ses_bc.bc_cl, CL_LEVEL_DEBUG, "%s:", title);
    cl_cover(ses->ses_bc.bc_cl);
    do {
      cl_log(ses->ses_bc.bc_cl, CL_LEVEL_DEBUG,
             "%s %s%s%s %s%s %s%s%s %s%s (%d) %s",
             ses->ses_bc.bc_have_priority ? "*" : " ",
             ses->ses_bc.bc_data_waiting_to_be_read ? "r" : "-",
             ses->ses_bc.bc_input_buffer_capacity_available ? "i" : "-",
             ses->ses_bc.bc_input_waiting_to_be_parsed ? "p" : "-",

             *ses->ses_request_input ? "p" : "-",
             ses->ses_request_head ? "h" : "-",

             ses->ses_bc.bc_output_buffer_capacity_available ? "f" : "-",
             ses->ses_bc.bc_output_waiting_to_be_written ? "o" : "-",
             ses->ses_bc.bc_write_capacity_available ? "w" : "-",
             ses->ses_bc.bc_error & SRV_BCERR_WRITE ? "W" : " ",
             ses->ses_bc.bc_error & SRV_BCERR_READ ? "R" : " ",
             (int)ses->ses_bc.bc_errno, ses->ses_displayname);

    } while ((ses = ses->ses_next) != chain);
  }
}

void srv_stat_sessions(srv_handle* srv) {
  cl_cover(srv->srv_cl);
  stat_chain(srv->srv_session_head, "sessions");
}
