#!/usr/bin/env python3

import os
from aws_cdk import core
from aws_automated_scheduler.automated_scheduler_stack import AutomatedSchedulerStack

# There is probably a better way to retrieve account ID's for deployment
dev = os.environ.get("AWS_DEV_ACCOUNT_ID", None)

app = core.App()
AutomatedSchedulerStack(app, "automated-scheduler-dev", env=core.Environment(account=dev,   region="us-west-2"))

app.synth()
