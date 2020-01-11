from aws_cdk import core
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_iam as iam

TABLE_NAME = 'Scheduler'
TAG_KEY = "Schedule"
REGION = "us-west-2"
LAMBDA_FUNC_PATH = 'aws_automated_scheduler/lambda'


class AutomatedSchedulerStack(core.Stack):
    """
    Setup stack for automated scheduler
    """

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Create a DynamoDB table with our requirements
        schedule_table = dynamodb.Table(
            self,
            "AutomatedSchedulerTable",
            table_name=TABLE_NAME,
            partition_key=dynamodb.Attribute(
                name="pk",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="sk",
                type=dynamodb.AttributeType.STRING
            ),
            removal_policy=core.RemovalPolicy.DESTROY
        )

        # Create lambda resource using code from local disk
        lambda_handler = _lambda.Function(
            self, "AutomatedScheduler",
            code=_lambda.Code.from_asset(LAMBDA_FUNC_PATH),
            runtime=_lambda.Runtime.PYTHON_3_7,
            handler="aws_scheduler.event_handler",
            memory_size=256,
            timeout=core.Duration.seconds(5)
        )

        schedule_table.grant_read_write_data(lambda_handler)

        lambda_handler.add_environment(
            key="scheduler_table",
            value=TABLE_NAME
        )

        lambda_handler.add_environment(
            key="scheduler_tag",
            value=TAG_KEY
        )

        lambda_handler.add_environment(
            key="scheduler_region",
            value=REGION
        )

        ec2_read_only = iam.PolicyStatement(
            actions=[
                "ec2:DescribeInstances",
                "ec2:DescribeTags"
            ],
            effect=iam.Effect.ALLOW,
            resources=[
                "*"
            ]
        )

        lambda_handler.add_to_role_policy(ec2_read_only)

    #  rule = events.Rule(
    #      self,
    #      "AutomatedSchedulerRule",
    #      schedule=events.Schedule.expression("cron(0/1 * * * ? *)")
    #  )

    # Add lambda function as target of event rule
    #   rule.add_target(targets.LambdaFunction(lambda_handler))
