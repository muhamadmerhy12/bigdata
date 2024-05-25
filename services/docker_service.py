import docker
from flask import current_app


class DockerService:
    def __init__(self):
        self.client = docker.from_env()
        self.check_containers()
        self.create_kafka_topic(current_app.config['KAFKA_RATINGS_TOPIC'])
        self.create_kafka_topic(current_app.config['KAFKA_RECOMMENDATIONS_TOPIC'])

    def check_containers(self):
        containers = self.client.containers.list()
        container_names = [container.name for container in containers]

        if current_app.config['DOCKER_ZOOKEEPER_CONTAINER_NAME'] not in container_names or \
                current_app.config['DOCKER_KAFKA_CONTAINER_NAME'] not in container_names:
            raise Exception("ZooKeeper and Kafka containers are not running. Please start them and try again.")

        print("ZooKeeper and Kafka containers are running.")

    def create_kafka_topic(self, topic_name):
        topic_creation_command = f"""
        kafka-topics --create --topic {topic_name} --bootstrap-server {current_app.config['KAFKA_BOOTSTRAP_SERVERS']} --partitions 1 --replication-factor 1
        """
        exec_log = self.client.containers.get(current_app.config['DOCKER_KAFKA_CONTAINER_NAME']).exec_run(
            cmd=topic_creation_command, tty=True, stdout=True, stderr=True)

        output = exec_log.output.decode("utf-8")
        if "already exists" in output:
            print(f"Topic '{topic_name}' already exists.")
        else:
            print(output)
