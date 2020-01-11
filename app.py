#!/usr/bin/env python3

from aws_cdk import core

from aws_automated_scheduler.aws_automated_scheduler_stack import AwsAutomatedSchedulerStack


app = core.App()
AwsAutomatedSchedulerStack(app, "aws-automated-scheduler")

app.synth()
