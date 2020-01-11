import pytest
import logging
import automated.ec2
import config

logger = logging.getLogger()


@pytest.fixture(name="ec2_manager", scope="class", autouse=True)
def ec2_manager_fixture():
    class EC2Manager:
        def __init__(self):
            self.client = automated.ec2.EC2(
                region="us-west-2",
                ec2_conn=config.EC2_CONN_LOCAL
            )

    return EC2Manager()


class TestEC2:
    """
    Most if not all of this requires interaction with AWS API's. Since our inputs are tags to filter off of and
    our responses are instances listed it is hard to develop tests around these.
    it is harder to verify most outputs
    """

    # TODO: Test large amount of intances to check paginator

    @pytest.fixture(scope="class", autouse=True)
    def setup_and_teardown(self):
        # LOAD INSTANCE DATA
        print("Setup\n")
        yield
        print("Stop\n")

    def test_get_instances(self):
        assert True