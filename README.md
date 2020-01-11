# AWS Automated Instance Scheduler

## Description

Starts/stops instances as appropriate using AWS tags as scheduling to tags based on scheduling configurations.

## Requirements
* Appropriate AWS access to create and configure the resources required
* Python >= 3.7
* pip (For installing Python requirements)
* Node.js >= 10.3.0 (For running AWS CDK)

## Resources created

This application creates the following resources:
(1) Lambda
(1) DynamoDB table
(1) CloudWatch Event rule

## Usage

This application leverages AWS CDK for stack creation and deployment. Node.js must be installed prior to using CDK.
        
https://docs.aws.amazon.com/cdk/latest/guide/getting_started.html

Once the appropriate version of Node.js is installed AWS CDK can be installed via  
```
npm install -g aws-cdk
```        
Installation can be verified by
```
cdk --version
```
After CDK installation is complete. Configure the Python virtual environment.        
```
$ python3 -m venv .env
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
$ source .env/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .env\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt
```

At this point you can now deploy the application using AWS CDK.
```
$ cdk deploy
```

# Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation

