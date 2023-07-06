# Deploying to MSK

The Ably Kafka Connector has been tested and works well on [AWS MSK](https://aws.amazon.com/msk/).
Here you'll find instructions on how to get started forwarding data from your MSK cluster to edge
devices using Ably via [MSK Connect](https://aws.amazon.com/msk/features/msk-connect/).

## Overview

A quick summary of what's needed to connect MSK to Ably:

* An MSK Kafka cluster, hosting the topic(s) you'd like to forward through Ably
* An MSK Connector configured to run a Custom Plugin, using the Ably Kafka Connector binary
* An [Ably account](https://ably.com/sign-up), with application and API key created with sufficient
  permissions to publish to the channel(s) required.

If you're an existing MSK user and would like to forward data to Ably, the high-level steps are:

* Obtain the latest plugin zip, either by:
  * Downloading it [here](https://sdk.ably.com/builds/ably/kafka-connect-ably/main/kafka-connect-ably-msk-plugin/kafka-connect-ably-3.0.0-bin.zip)
  * Checking out this repo and running `mvn clean package`, the zip will be at `target/kafka-connect-ably-msk-plugin`
* Put the plugin zip somewhere your MSK Connector can access it, e.g. an S3 bucket that you control
  * Note: MSK Connect will only download plugin binaries from S3 buckets in the same region as the connector, so
    downloading directly from Ably will only work in `eu-west-2`
* Follow the AWS documentation to add a [Custom Plugin](https://docs.aws.amazon.com/msk/latest/developerguide/msk-connect-plugins.html) 
  to your cluster, referencing the plugin zip above.
* See [configuration](https://github.com/ably/kafka-connect-ably/#configuration-properties) documentation
  to adjust settings as required.


## Example

This directory contains a working example that should be repeatable to [Terraform](https://www.terraform.io/) users.
The Terraform configuration in this directory will create:

* A VPC and necessary networking config to host all components
* An MSK Kafka cluster
* An MSK Connector using the Ably plugin
* A bastion host that can be used to access the connector for testing purposes

Take a look at `variables.tf` to see if there's any configuration you'd like to override. The `ably_client_key` 
is required and sensitive, you need to generate a key from your Ably account to be used by the connect. You can
then run this example using:

```
$ export TF_VAR_ably_client_key=<your ably key>
$ terraform init
$ terraform apply
```

You will most-likely want to change:

* The region being deployed to, and then the location of the plugin binary if running in another region
* The EC2 key-pair name being used to access the bastion
  * (or adapt this to your own team conventions)
