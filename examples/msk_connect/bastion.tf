# Resources to provision a bastion host in a public subnet,
# which has a Kafka distribution installed and can be used to
# run CLI tools to interact with the Kafka cluster.
#
# This example makes use of a .pem that is assumed to already
# exist, and the name is provided as the `bastion_ec2_key_pair`
# input variable.


# Select the latest available Amazon Linux 2 image
data "aws_ami" "amazon_linux_2" {
 most_recent = true
 owners = ["amazon"]
 filter {
   name = "owner-alias"
   values = ["amazon"]
 }
 filter {
   name = "name"
   values = ["amzn2-ami-hvm*"]
 }
}

# Bastion host EC2 instance
resource "aws_instance" "bastion_host" {
  # Depending on Kafka cluster, as we want to write the broker addresses
  # into a file on the bastion, so that the CLI tools can make use of it
  depends_on = [aws_msk_cluster.kafka]

  ami = data.aws_ami.amazon_linux_2.id
  instance_type = "t2.micro"
  key_name = var.bastion_ec2_key_pair
  subnet_id = module.vpc.public_subnets[0]
  vpc_security_group_ids = [aws_security_group.bastion_host.id]
  associate_public_ip_address = true
  user_data = templatefile("bastion.tftpl", {
    bootstrap_server_1 = split(",", aws_msk_cluster.kafka.bootstrap_brokers)[0]
    bootstrap_server_2 = split(",", aws_msk_cluster.kafka.bootstrap_brokers)[1]
    bootstrap_server_3 = split(",", aws_msk_cluster.kafka.bootstrap_brokers)[2]
  })
  root_block_device {
    volume_type = "gp2"
    volume_size = 100
  }
  tags = {
    Name = "${var.global_prefix}-bastion-host"
  }
}

# Print the SSH command needed to access the bastion as an output
output "bastion_ssh_cmd" {
    value = "ssh ec2-user@${aws_instance.bastion_host.public_ip} -i ~/.ssh/${var.bastion_ec2_key_pair}.pem"
}
