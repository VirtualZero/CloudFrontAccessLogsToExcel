import boto3
import gzip
import os
import re
import concurrent.futures
import pendulum
import pandas as pd
import tarfile
import json


s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')
log_bucket = os.environ['LOG_BUCKET']
log_prefix = os.environ['LOG_PREFIX']
destination_bucket = os.environ['DESTINATION_BUCKET']
destination_prefix = os.environ['DESTINATION_PREFIX']
log_bucket_resource = s3_resource.Bucket(log_bucket)
temp_dir = '/tmp'
master_log = ('Date\t'
              'Time\t'
              'Edge Location\t'
              'Response Bytes\t'
              'IP\t'
              'Method\t'
              'Host\t'
              'Path\t'
              'Status Code\t'
              'Referer\t'
              'User-Agent\t'
              'Query String\t'
              'Cookie\t'
              'Cache Status\t'
              'Edge Request ID\t'
              'Host Header\t'
              'Protocol\t'
              'Request Byes\t'
              'Total Time\t'
              'Forwarded For\t'
              'SSL Protocol\t'
              'SSL Cipher\t'
              'Edge Response Result\t'
              'HTTP Version\t'
              'FLE Status\t'
              'FLE Encrypted Fields\t'
              'Port\t'
              'Time to First Byte\t'
              'Detailed Cache Status\t'
              'Content Type\t'
              'Content Length\t'
              'Content Range Start\t'
              'Content Range End')


class Files():
    @staticmethod
    def write_file(file, content):
        with open(file, 'w+') as f:
            f.write(content)
        return True


    @staticmethod
    def delete_file(file):
        if os.path.exists(file) and os.path.isfile(file):
            os.remove(file)


    @staticmethod
    def make_tarfile(source, destination):
        with tarfile.open(destination, 'w:gz') as tar:
            tar.add(source)


class AWS():
    @staticmethod
    def get_log_keys():
        return [log.key for log in log_bucket_resource.objects.filter(Prefix=log_prefix)]


    @staticmethod
    def download_log(key):
        s3_client.download_file(
            log_bucket, 
            key, 
            os.path.join(
                temp_dir,
                re.match('.*/(.*)', key).group(1)
            ))


    @staticmethod
    def upload(local_path, bucket_name, s3_key, content_type):
        s3 = boto3.client('s3')
        return s3.upload_file(
            local_path,
            bucket_name,
            s3_key,
            ExtraArgs={
                'ContentType': content_type
            }
        )


    @staticmethod
    def delete_logs():
        log_bucket_resource.objects.filter(Prefix=log_prefix).delete()


class LogUtils():
    @staticmethod
    def aggregate():
        logs = [os.path.join(
            temp_dir,
            log
        ) for log in os.listdir(temp_dir)]
        
        for log in logs:
            LogUtils.append(log)


    @staticmethod
    def append(log):
        global master_log
        with gzip.open(log, 'r') as gzip_log:
            lines = [line.decode('utf-8').strip()
                    for line in gzip_log.readlines()
                    if line.decode('utf-8').strip()[0] != '#']
        for line in lines:
            master_log = f'{master_log}\n{line}'


    @staticmethod
    def write_excel():
        csv_file = os.path.join(
            temp_dir,
            'log.csv'
        )
        excel_file = os.path.join(
            temp_dir,
            f"{pendulum.now('utc').to_datetime_string().replace(' ', '_').replace(':', '-')}_utc.xlsx"
        )
        Files.write_file(csv_file, master_log)
        df = pd.read_csv(csv_file, sep='\t')
        df.to_excel(excel_file)
        return excel_file


    @staticmethod
    def delete_local_logs():
        files = [os.path.join(
            temp_dir,
            file
        ) for file in os.listdir(temp_dir)]
        threaded(Files.delete_file, files)


    @staticmethod
    def delete_remote_logs():
        AWS.delete_logs()


    @staticmethod
    def convert_to_excel():
        LogUtils.aggregate()
        return LogUtils.write_excel()


    @staticmethod
    def tar_excel(excel_file):
        excel_tar = f'{excel_file}.tgz'
        Files.make_tarfile(excel_file, excel_tar)
        return excel_tar


    @staticmethod
    def upload_tar(excel_tar):
        s3_key = os.path.join(
            destination_prefix,
            re.match('.*/(.*)', excel_tar).group(1)
        )
        AWS.upload(excel_tar, destination_bucket, s3_key, 'application/gzip')


    @staticmethod
    def download_logs():
        threaded(AWS.download_log, AWS.get_log_keys())


def threaded(func, _list):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(func, _list)


def lambda_handler(event, context):
    LogUtils.download_logs()
    excel_file = LogUtils.convert_to_excel()
    excel_tar = LogUtils.tar_excel(excel_file)
    LogUtils.upload_tar(excel_tar)
    LogUtils.delete_local_logs()
    LogUtils.delete_remote_logs()
    
    return json.dumps(
        {'status': 'success'}
    )