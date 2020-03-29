import boto3
import gzip
import os
import re
import concurrent.futures
import argparse
import pendulum
import shutil
import pandas as pd
from halo import Halo


s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')
log_bucket = 'BUCKET_NAME'
log_bucket_obj = s3_resource.Bucket(log_bucket)
log_prefix = 'BUCKET_PREFIX'
root_dir = os.getcwd()
temp_dir = os.path.join(
    root_dir,
    'temp_access_logs'
)
access_logs_dir = os.path.join(
    root_dir,
    'access_logs'
)
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
    def load_file(file):
        with open(file, 'r') as f:
            return f.read()


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
    def delete_dir(dir):
        if os.path.exists(dir) and os.path.isdir(dir):
            os.rmdir(dir)


    @staticmethod
    def force_delete_dir(dir):
        if os.path.exists(dir) and os.path.isdir(dir):
            shutil.rmtree(dir)


class AWS():
    @staticmethod
    def get_log_keys():
        return [log.key for log in log_bucket_obj.objects.filter(Prefix=log_prefix)]


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
    def delete_logs():
        log_bucket_obj.objects.filter(Prefix=log_prefix).delete()


class LogUtils():
    @staticmethod
    def aggregate():
        with Halo(
            text=f"Aggregating Access Logs",
            spinner='dots',
            text_color='white',
            color='green'
        ):
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
        with Halo(
            text=f"Creating Spreadsheet",
            spinner='dots',
            text_color='white',
            color='green'
        ):
            csv_file = os.path.join(
                temp_dir,
                'log.csv'
            )
            excel_file = os.path.join(
                access_logs_dir,
                f"{pendulum.now('utc').to_datetime_string().replace(' ', '_').replace(':', '-')}_utc.xlsx"
            )
            Files.write_file(csv_file, master_log)
            df = pd.read_csv(csv_file, sep='\t')
            df.to_excel(excel_file)


    @staticmethod
    def delete_local_logs():
        with Halo(
            text=f"Deleting Downloaded Logs",
            spinner='dots',
            text_color='white',
            color='green'
        ):
            Files.force_delete_dir(temp_dir)


    @staticmethod
    def delete_remote_logs():
        with Halo(
            text=f"Deleting S3 Logs",
            spinner='dots',
            text_color='white',
            color='green'
        ):
            AWS.delete_logs()


    @staticmethod
    def convert_to_excel():
        os.makedirs(access_logs_dir, exist_ok=True)
        LogUtils.aggregate()
        LogUtils.write_excel()


    @staticmethod
    def download_logs():
        with Halo(
            text=f"Downloading Access Logs",
            spinner='dots',
            text_color='white',
            color='green'
        ):
            os.makedirs(temp_dir, exist_ok=True)
            threaded(AWS.download_log, AWS.get_log_keys())


def parse_args():
    parser = argparse.ArgumentParser(
        description="CloudFront access log parser and Excel spreadsheet generator."
    )

    parser.add_argument(
        '-d',
        '--download-logs',
        help=f'Download CloudFront access logs from S3.',
        action='store_true'
    )

    parser.add_argument(
        '-x',
        '--convert-to-excel',
        help=f'Convert downloaded CloudFront access logs to Excel spreadsheet.',
        action='store_true'
    )

    parser.add_argument(
        '-b',
        '--batch-operation',
        help=f'Performs all operations.',
        action='store_true'
    )

    parser.add_argument(
        '-r',
        '--delete-remote',
        help=f'Deletes all CloudFront access logs from S3 bucket.',
        action='store_true'
    )

    parser.add_argument(
        '-l',
        '--delete-local',
        help=f'deletes all downloaded CloudFront access logs and directory.',
        action='store_true'
    )

    return parser.parse_args()


def threaded(func, _list):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(func, _list)


if __name__ == '__main__':
    args = parse_args()

    if args.download_logs:
        LogUtils.download_logs()

    if args.delete_remote:
        LogUtils.delete_remote_logs()

    if args.delete_local:
        LogUtils.delete_local_logs()

    if args.convert_to_excel:
        LogUtils.convert_to_excel()

    if args.batch_operation:
        LogUtils.download_logs()
        LogUtils.convert_to_excel()
        LogUtils.delete_local_logs()
        LogUtils.delete_remote_logs()

    print('\nDone.')
