#!/usr/bin/env python
import os, sys
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from subprocess import Popen, PIPE
import StringIO

#SETTINGS
from backup_settings import *

# Returns the value of a django manage.py dumpdata command as a string.
def get_dumped_data(*apps, **options):
    from django.core.management import call_command
    output = StringIO.StringIO()

    # Redirect stdout to output var
    sys.stdout = output
    call_command('dumpdata', *apps, **options)
    sys.stdout = sys.__stdout__
    
    o = output.getvalue()
    output.close()
    o.rstrip()
    return o

# Returns output of a postgres dump as a string
def get_postgres_dump(dbname):
    process = Popen(["pg_dump", dbname], stdout=PIPE)
    output = process.communicate()[0]
    return output
    
# Returns a connection, bucket, and/or key for an s3 account
# check out usage below
def s3_init(access_key_id, secret_key, bucket_name=None, key_name=None):
    conn = S3Connection(access_key_id, secret_key)
    if bucket_name is not None:
        bucket = conn.create_bucket(bucket_name)
        if key_name is not None:
            key = Key(bucket)
            key.key = key_name
            return conn, bucket, key
        else:
            return conn, bucket
    else:
        return conn, bucket, key

if __name__ == '__main__':
    #init pypath and environ vars
    sys.path.extend([PROJECT_DIR, PROJECT_DIR+'/..'])
    sys.path.insert(0, PROJECT_DIR+'/externals')
    os.environ['DJANGO_SETTINGS_MODULE'] = SETTINGS_PYPATH
    from django.conf import settings

    # Get the dumps as strings
    print("Dumping data.")
    dumped_data = get_dumped_data()
    pg_dump = get_postgres_dump(settings.DATABASE_NAME)

    #These are the strings (and their key names) that will get backed up.
    # To back up more stuff, just add to this dict.
    local_data = {'dumpdata': dumped_data, 'pg_dump': pg_dump}

    # Initialize S3 connection
    print("Connecting to AWS.")
    conn, bucket = s3_init(AWS_ACCESS_KEY_ID, AWS_SECRET_KEY, BUCKET_NAME)

    import hashlib
    # For each thing to back up, back it up
    for label, data in local_data.items():
        # get key
        key = Key(bucket)
        key.key = label
        # local hash
        local_md5 = hashlib.md5(data).hexdigest()
        key.open(); key.close(); #loads key.etag
        # remote hash
        remote_md5 = key.etag.replace('\"','') # clears quote marks
        print("*** %s ***" % label)
        print("\t*LOCAL*")
        print("\t\tmd5: %s" % local_md5)
        print("\t\tcontents: %s" % data[:25])
        print("\t*REMOTE*")
        print("\t\tmd5: %s" % remote_md5)
        print("\t\tcontents: %s" % key.get_contents_as_string()[:25])
        # If new backup is different than the last saved one, update it
        if local_md5 != remote_md5:
            print("\tMD5 values different; uploading new version.")
            key.set_contents_from_string(data)
            print("\tKey '%s' updated." % key.name)
            print("\t*NEW REMOTE*")
            print("\t\tmd5: %s" % key.md5)
            print("\t\tcontents: %s" % key.get_contents_as_string()[:25])
        else:
            print("MD5s were the same. Nothing changed.")
