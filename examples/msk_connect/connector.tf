# An MSK Connector resource, plus related IAM resources.
# This adds the Ably Kafka Connector to an MSK Kafka cluster.

# The Connector needs an IAM role to assume in order to access
# AWS services related to its functionality. The Ably plugin doesn't
# use any AWS services, as it simply forwards data from Kafka topics
# to Ably. This role will need to provide access to the S3 bucket where
# the plugin zip is stored though, if not using a public bucket.
resource "aws_iam_role" "connector_role" {
  name = "${var.global_prefix}-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        "Effect": "Allow",
        "Principal": {
          "Service": "kafkaconnect.amazonaws.com"
        },
        "Action": "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "connector_role_policy" {
  name = "${var.global_prefix}-role-policy"
  role = aws_iam_role.connector_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        "Effect": "Allow",
        "Action": [
          "s3:ListAllMyBuckets"
        ],
        "Resource": "arn:aws:s3:::*"
      },
      {
        "Effect": "Allow",
        "Action": [
          "s3:GetBucketLocation",
          "s3:ListBucket",
          "s3:GetObject",
        ],
        "Resource": var.connector_binaries_bucket_arn
      }
    ]
  })
}

# Resource for a custom plugin, which is how we install the Ably
# plugin into an MSK Connector. Note that the plugin is ZIP type,
# as it contains the Ably connector JAR plus dependencies.
resource "aws_mskconnect_custom_plugin" "plugin" {
  name = "${var.global_prefix}-plugin"
  content_type = "ZIP"
  location {
    s3 {
      bucket_arn = var.connector_binaries_bucket_arn
      file_key = var.connector_binary_key
    }
  }
}

# MSK Connector resource with the custom plugin above referenced.
# Most of the settings here can be tweaked to suit user needs.
resource "aws_mskconnect_connector" "connector" {
  name = "${var.global_prefix}-connector"
  kafkaconnect_version = "2.7.1"

  # Note: Autoscaling may not be needed. Settings will likely
  # need tuning to suit the workload.
  capacity {
    autoscaling {
      mcu_count = 1
      min_worker_count = 1
      max_worker_count = 3
      scale_in_policy {
        cpu_utilization_percentage = 20
      }
      scale_out_policy {
        cpu_utilization_percentage = 80
      }
    }
  }

  # Configuration here will go to the connector properties file.
  # It may be useful to expose more as input variables.
  connector_configuration = {
    # Note: Connector does support schemas, but we don't have a 
    # schema registry in this example deployment
    "connector.class" = "com.ably.kafka.connect.ChannelSinkConnector"
    "key.converter" = "org.apache.kafka.connect.converters.ByteArrayConverter"
    "value.converter" = "org.apache.kafka.connect.converters.ByteArrayConverter"
    "value.converter.schemas.enable" = "false"
    "tasks.max" = "3"
    "topics" = var.source_topics
    "channel" = var.dest_ably_channel_pattern
    "client.id" = var.ably_client_id
    "client.key" = var.ably_client_key
  }

  # Reference the Kafka cluster configured in kafka.tf
  kafka_cluster {
    apache_kafka_cluster {
      bootstrap_servers = aws_msk_cluster.kafka.bootstrap_brokers
      vpc {
        security_groups = [aws_security_group.connector.id]
        subnets = module.vpc.private_subnets
      }
    }
  }
  kafka_cluster_client_authentication {
    authentication_type = "NONE"
  }
  kafka_cluster_encryption_in_transit {
    encryption_type = "PLAINTEXT"
  }

  # Attach the plugin defined above
  plugin {
    custom_plugin {
      arn = aws_mskconnect_custom_plugin.plugin.arn
      revision = aws_mskconnect_custom_plugin.plugin.latest_revision
    }
  }

  # This is the IAM role discussed above -- needed to access
  # the plugin zip file in S3
  service_execution_role_arn = aws_iam_role.connector_role.arn
}
