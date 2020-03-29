# CloudFrontAccessLogsToExcel
Powered by [VirtualZero](https://blog.virtualzero.tech "VirtualZero's Blog Website")

When AWS CloudFront is configured to deliver access logs, it can be a chore to parse, aggregate, and organize all of the data. Each access log is ~5 KB and thousands can easily stack up within a week. CloudFrontAccessLogsToExcel offers 2 solutions: a cloud solution that will run without intervention on a schedule defined by a Cron expression and a CLI version for local use. Both versions download all of the access logs from a specified bucket, uncompress each log, read each log, append its data to the master log, convert the master log format from CSV to XLSX, save the XLSX file, delete the downloaded access logs, and delete the access logs from the S3 bucket that the logs are stored in. The CLI version allows for many of these operations to be performed individually, while the cloud version will save the generated XLSX file in a S3 bucket. Many of the operations performed in the script are threaded to increase performance and reduce run time.

## Cloud Solution
### 1. Upload the zip file located in aws/lambda to a S3 bucket

### 2. Note the uploaded zip file's bucket name, key, and version ID

### 3. Upload the CloudFormation template located in aws/cloudformation to Cloudformation

### 4. Enter the required information into the form

### 5. CloudFormation will provision all of the resources and handle all permissions

#### Resources created by the CloudFormation template:

* IAM Role
* IAM Managed Policy
* Lambda Function
* CloudWatch Rule
* CloudWatch to Lambda Permission

## CLI Solution
### 1. Install the dependencies

```sh
cd CloudFrontAccessLogsToExcel && pipenv install
```

### 2. Execute the script

```sh
pipenv run python cli/access_logs_to_excel_cli.py -b
```

### Usage
#### Flags

| Flag | Operation |
| :--- | :--- |
| -h<br>&#45;&#45;help | Displays the help menu, flags, and flag descriptions |
| -d<br>&#45;&#45;download&#45;logs | Download CloudFront access logs from S3 |
| -x<br>&#45;&#45;convert&#45;to&#45;excel | Convert downloaded CloudFront access logs to Excel spreadsheet |
| -b<br>&#45;&#45;batch&#45;operation | Performs all operations, most common usage |
| -r<br>&#45;&#45;delete&#45;remote | Deletes all CloudFront access logs from S3 bucket |
| -l<br>&#45;&#45;delete&#45;local | Deletes all downloaded CloudFront access logs and directory |
