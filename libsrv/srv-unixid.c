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
/*
 * srv-unixid.c -- look up uids and gids
 */

#include <errno.h>
#include <pwd.h>
#include <grp.h>

#include "srvp.h"

int srv_unixid_name_to_uid(char const *name, uid_t *uid) {
  struct passwd *pw;

  errno = 0;

  pw = getpwnam(name);
  if (pw == NULL) return errno ? errno : SRV_ERR_NO;

  *uid = pw->pw_uid;
  endpwent();

  return 0;
}

int srv_unixid_name_to_gid(char const *name, gid_t *gid) {
  struct group *grent;

  grent = getgrnam(name);
  if (grent == NULL) return errno ? errno : SRV_ERR_NO;

  *gid = grent->gr_gid;
  endgrent();

  return 0;
}

int srv_unixid_become(uid_t uid, gid_t gid) {
  /* Use setuid(), not seteuid() -- we don't want there
   * to be a way back to privilege.
   *
   * If we're not root, it's not an error for these
   * to fail -- we'll just run as the user, and that's okay.
   */
  (void)setuid(uid);
  (void)setgid(gid);

  return 0;
}
