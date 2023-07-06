# Resources to set up an MSK Kafka cluster, which we can
# use with MSK Connect to send data to Ably

# Encryption key used for data storage on the Kafka cluster
resource "aws_kms_key" "kafka_kms_key" {
  description = "Key for Apache Kafka"
}

# Kafka properties can be changed or added here
resource "aws_msk_configuration" "kafka_config" {
  kafka_versions = ["3.4.0"]
  name = "${var.global_prefix}-config"
  server_properties = <<EOF
auto.create.topics.enable = true
delete.topic.enable = true
EOF
}

# It's useful, for debugging, to be able to access the Kafka
# logs in Cloudfront
resource "aws_cloudwatch_log_group" "kafka_log_group" {
  name = "kafka_broker_logs"
}

# Kafka cluster resource
resource "aws_msk_cluster" "kafka" {
  cluster_name = var.global_prefix
  kafka_version = "3.4.0"
  number_of_broker_nodes = 3
  broker_node_group_info {
    instance_type = "kafka.m5.large"
    storage_info {
      ebs_storage_info {
        volume_size = 1000
      }
    }
    # The Kafka cluster itself can run on a private subnet, it
    # shouldn't need any public access
    client_subnets =  module.vpc.private_subnets
    security_groups = [aws_security_group.kafka.id]
  }
  encryption_info {
    encryption_in_transit {
      client_broker = "PLAINTEXT"
    }
    encryption_at_rest_kms_key_arn = aws_kms_key.kafka_kms_key.arn
  }
  configuration_info {
    arn = aws_msk_configuration.kafka_config.arn
    revision = aws_msk_configuration.kafka_config.latest_revision
  }
  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled = true
        log_group = aws_cloudwatch_log_group.kafka_log_group.name
      }
    }
  }
  tags = {
    name = var.global_prefix
  }
}
