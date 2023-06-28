terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.3"
    }
  }

  required_version = ">= 1.2.0"
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      project = var.global_project_tag
    }
  }
}
