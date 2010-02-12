#!/usr/bin/env python
import os, sys
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import StringIO

#SETTINGS
#S3
AWS_ACCESS_KEY_ID = 'AKIAIDCCM2YY4GTCKZOA'
AWS_SECRET_KEY = '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'
BUCKET_NAME = 'kbs_kars'
KEY_NAME = 'datadump_backup'
#DJANGO
PROJECT_DIR = '/var/www/karscode'
SETTINGS_PYPATH = 'karscode.settings.kars'

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

    print("Dumping data.")
    dumped_data = get_dumped_data('kbs_publications')
    print("Connecting to AWS.")
    conn, bucket, key = s3_init(AWS_ACCESS_KEY_ID, AWS_SECRET_KEY, 'kbs_kars', 'dumpdata_backup')

    import hashlib
    local_md5 = hashlib.md5(dumped_data).hexdigest()
    key.open()
    key.close() #loads key.etag
    remote_md5 = key.etag.replace('\"','')

    print("***LOCAL***")
    print("\tmd5: %s" % local_md5)
    print("\tcontents: %s" % dumped_data[:25])
    print("***REMOTE***")
    print("\tmd5: %s" % remote_md5)
    print("\tcontents: %s" % key.get_contents_as_string()[:25])

    if local_md5 != remote_md5:
        print("md5 values different. Uploading new version.") 
        key.set_contents_from_string(dumped_data)
        print("Key '%s' updated." % (key.name))
        print("***NEWREMOTE***")
        print("\tmd5: %s" % key.md5)
        print("\tcontents: %s" % key.get_contents_as_string()[:25])
    else:
        print("md5s were the same. Nothing changed.")
