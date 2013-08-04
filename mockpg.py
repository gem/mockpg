#!/usr/bin/env python
# Copyright (c) 2013, GEM Foundation.
#
# mockpg is free software: you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# mockpg is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with OpenQuake.  If not, see <http://www.gnu.org/licenses/>.

import pexpect
import sys
import os
import time
import struct
import ast

SOCKDIR="/tmp/mockpg/"
SOCK=SOCKDIR+".s.PGSQL.5432"

DEBUG=False

exps = [ pexpect.EOF, pexpect.TIMEOUT,
         "\0\0\0T\0\3\0\0user\0postgres\0database\0postgres\0application_name\0psql\0client_encoding\0UTF8\0\0",
         "X\0\0\0\4"
         ]

reps = [ None, None,
         "R\0\0\0\10\0\0\0\0S\0\0\0\32application_name\0psql\0S\0\0\0\31client_encoding\0UTF8\0S\0\0\0\27DateStyle\0ISO, MDY\0S\0\0\0\31integer_datetimes\0on\0S\0\0\0\33IntervalStyle\0postgres\0S\0\0\0\24is_superuser\0on\0S\0\0\0\31server_encoding\0UTF8\0S\0\0\0\31server_version\0009.1.9\0S\0\0\0#session_authorization\0postgres\0S\0\0\0$standard_conforming_strings\0off\0S\0\0\0\27TimeZone\0localtime\0K\0\0\0\f\0\0|9\0302c8Z\0\0\0\5I",
         None
         ]

#
#  exp = string
#  rep = [ [ desc1, descN ... ], [ row1:field1, row1:field2, ... ], [row2:field1, row2:field2, ... ] ...  ]
#
def populate (exp, rep):
    exps.append( "Q"+struct.pack(">i", len(exp) +4 + 1)+exp+"\0")
    
    t_fields = "" + struct.pack('>h', len(rep[0]))
    for t_field in rep[0]:
        t_fields = t_fields + t_field + '\x00' + struct.pack(">i", 43) + struct.pack(">h", 3840)
        t_fields = t_fields + struct.pack(">i", 33554432) + struct.pack(">h", 0) 
        t_fields = t_fields + struct.pack(">i", -1) + struct.pack(">h", 0)
    t = "T" + struct.pack(">i", len(t_fields)+4) + t_fields

    d = ""
    for i in range(1, len(rep)):
        if len(rep[0]) != len(rep[i]):
            print "MALFORMED POPULATE: %s %s" % (repr(exp), repr(rep))
            return 1

        d_fields = struct.pack('>h', len(rep[i]))
        for d_field in rep[i]:
            d_fields = d_fields + struct.pack(">i", len(d_field)) + d_field

        d = d + "D" + struct.pack(">i", len(d_fields) + 4) + d_fields
            
    c = "C\0\0\0\rSELECT 1\0"
    z = "Z\0\0\0\5I"

    mesg = t + d + c + z
    if DEBUG:
        print "Populate:"
        print "MESG: T: [%s]" % repr(t)
        print "MESG: D: [%s]" % repr(d)
        print "MESG: C: [%s]" % repr(c)
        print "MESG: Z: [%s]" % repr(z)
        print
        
        # print repr(mesg)
        # print
        
    reps.append(mesg)


# The response is the composition of 3 type of records 'T' (header desc), 'D' (data rows), 'C' (close)
#
# T message:   'T' , I32 total len of the mesg (without 'T') , I16 number of fields
#                  for each field:
#                      String Field name (null terminated)
#                      I32 If the field can be identified as a column of a specific table, 
#                          the object ID of the table; otherwise zero.
#                      I16 If the field can be identified as a column of a specific table, 
#                          the attribute number of the column; otherwise zero.
#                      I32 The object ID of the field's data type.
#                      I16 The data type size (see pg_type.typlen). Note that negative values denote
#                           variable-width types.
#                      I32 The type modifier (see pg_attribute.atttypmod). The meaning of the modifier
#                          is type-specific.
#                      I16 The format code being used for the field. Currently will be zero (text) or
#                          one (binary). In a RowDescription returned from the statement variant of Describe, 
#                          the format code is not yet known and will always be zero.
#                      
# D message:   'D',  I32 total len of the mesg (without 'D'), I16 number of cols
#                  for each field:
#                      I32 len of field (-1 means NULL and no other bytes must be added)
#                      Bytes the field value

#
#  MAIN
#

if len(sys.argv) < 3 or ((len(sys.argv) - 1) % 2) != 0:
    print "Usage:"
    print "    %s <match> <return> [<match> <return> [<match> <return> ... ]]" % sys.arg[0]
    print "    <match>  - the query addressed"
    print "    <return> - a parsable string python object in the form [ [ descs ], [ first row ], ... ]"
    sys.exit(1)

# populate( "SELECT setting from pg_settings where name = 'garago';", [ [ "setting" ] , [ "true" ] ] );
# populate( "SELECT setting from pg_settings where name = 'gara';", [ [ "allo" ] , [ "tollo" ] ] );
# populate( "SELECT a,b,c from pg_settings where name = 'ga';", [ [ "a", "b", "c" ] , [ "allo", "billo", "collo" ] ] );
# populate( "SELECT name,setting from pg_settings WHERE name = 'application_name';", [ [ "name", "setting" ], ["application_name", "psql" ], ["application_sguzzo", "psqlzzo" ]] )

for i in range( 1, len(sys.argv), 2):
    populate(sys.argv[i], ast.literal_eval(sys.argv[i+1]))

# umask change is required to drive netcat to create a unix socket accessible from any user
umask_old = os.umask(0)
child = pexpect.spawn('nc -l -U '+SOCK, timeout=30, maxread=1)
os.umask(umask_old)

if not os.path.isdir(SOCKDIR):
    if os.access(SOCKDIR, os.R_OK):
        os.unlink(SOCKDIR)
    os.mkdir(SOCKDIR)

if os.access(SOCK, os.R_OK):
    os.remove(SOCK)

# fout = file('pexpect.log','w')
# child.logfile = fout

pexpect.tty.setraw(child.fileno())
print "READY"
err = False
while True:
    # print "pre exp"
    r = child.expect_exact (exps)
    if DEBUG:
        print "Received: [%s]" % repr(exps[r])

    if r < 2:
        err = True
        break
    elif r == 3:
        finished = True

    if reps[r] != None:
        if DEBUG:
            print "Sent: [%s]" % repr(reps[r])
        sent = child.send(reps[r])
        if sent != len(reps[r]):
            # r == 3 implies normal close
            err = True
            break
        # print "SEND REP [%d]" % sent
        child.flush()
    else:
        child.sendeof()
    
child.close()

if not finished and err:
    print "Error with index %d" % r
    sys.exit(1)
# print "post send"
sys.exit(0)
