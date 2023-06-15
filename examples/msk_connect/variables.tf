variable "global_prefix" {
    description = "A string prefix used globally to name things"
    type = string
    default = "ably-kafka-connect-example"
}

variable "global_project_tag" {
    description = "A `project` tag that will be added to all resources"
    type = string
    default = "ably-kafka-connect-example"
}

variable "region" {
    description = "AWS region to deploy to"
    type = string
    default = "us-west-2"
}

variable "bastion_ec2_key_pair" {
    description = "An existing EC2 key pair used for SSH access"
    type = string
    default = "ably-msk-connector-key"
}

variable "source_topics" {
    description = "Kafka topics to forward to Ably"
    type = string
    default = "ably-connector-test-topic"
}

variable "dest_ably_channel_pattern" {
    description = "Ably channel name, or substition pattern, to send records to"
    type = string
    default = "kafka-connect-ably-example"
}

variable "ably_client_id" {
    description = "An identifier to be used as the Ably client ID forwarding messages"
    type = string
    default = "ably-kafka-connector"
}

variable "ably_client_key" {
    description = "Secret Key to use to connect to Ably"
    type = string
    sensitive = true
}

# TODO: Set up a disribution zip for MSK in sdk.ably.com bucket and point
# to that, rather than using a dedicated bucket here
variable "connector_binaries_bucket_arn" {
    description = "S3 bucket ARN to download connector plugin zip from"
    type = string
    default = "arn:aws:s3:::ably-kafka-connect-example"
}

variable "connector_binary_key" {
    description = "S3 Key for connector plugin zip, within connector_binaries_bucket"
    type = string
    default = "kafka-connect-ably-standalone.zip"
}
