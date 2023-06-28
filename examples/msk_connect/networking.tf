# As we need to provision a lot of resources into specific subnets,
# the approach taken here is to just setup a dedicated VPC so that the
# example can be reproduced safely, as long as VPC limits have not been
# reached in the given region.
module "vpc" {
  source = "terraform-aws-modules/vpc/aws"
  version = "5.0.0"

  name = "${var.global_prefix}-vpc"
  cidr = "10.0.0.0/16"

  azs = ["${var.region}a", "${var.region}b", "${var.region}c"]
  private_subnets = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
  public_subnets = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]

  enable_nat_gateway = true
  enable_vpn_gateway = false
}

# A secruity group for the Kafka cluster, to enable Kafka traffic from the bastion
# instance (public subnets) and other Kafka instances (private subnets), but block
# everything else.
resource "aws_security_group" "kafka" {
  name = "${var.global_prefix}-kafka"
  vpc_id = module.vpc.vpc_id
  ingress {
    from_port = 0
    to_port = 9092
    protocol = "TCP"
    cidr_blocks = ["10.0.1.0/24",
                   "10.0.2.0/24",
                   "10.0.3.0/24"]
  }
  ingress {
    from_port = 0
    to_port = 9092
    protocol = "TCP"
    cidr_blocks = ["10.0.101.0/24", 
                   "10.0.102.0/24",
                   "10.0.103.0/24"]
  }
  egress {
    from_port = 0
    to_port = 0
    protocol = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = {
    Name = "${var.global_prefix}-kafka"
  }
}

# Note: This bastion host is configured to allow public SSH access.
# If you follow different conventions, change the bastion host setup
# to whatever works for your organsation.
resource "aws_security_group" "bastion_host" {
  name = "${var.global_prefix}-bastion-host"
  vpc_id = module.vpc.vpc_id
  ingress {
    from_port = 22
    to_port = 22
    protocol = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = {
    Name = "${var.global_prefix}-bastion-host"
  }
}

# Connector only needs outgoing internet access, to upload to Ably
resource "aws_security_group" "connector" {
  name = "${var.global_prefix}-connector"
  vpc_id = module.vpc.vpc_id
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = {
    Name = "${var.global_prefix}-connector"
  }
}
