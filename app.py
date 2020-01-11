#!/usr/bin/env python3

from aws_cdk import core

from aws_automated_scheduler.automated_scheduler_stack import AutomatedSchedulerStack


app = core.App()
AutomatedSchedulerStack(app, "automated-scheduler", env=core.Environment(account="dev",   region="us-west-2"))

app.synth()
