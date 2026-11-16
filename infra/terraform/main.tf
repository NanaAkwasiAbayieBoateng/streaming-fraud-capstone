# Streaming Fraud Capstone — single-VM skeleton (NOT applied).
#
# This is a starting point for the day the project outgrows a laptop:
# one small free-tier-oriented VM running the whole stack (Redpanda +
# Bytewax scoring app + SQLite feature store) behind Docker Compose.
# It deliberately provisions ONE instance — no Kubernetes, no managed
# streaming service — because at this project's scale a single VM is
# the cheapest thing that works (see ../../docs/tradeoffs.md).
#
# No provider credentials are committed here, ever. Authenticate with
# the cloud CLI / environment variables locally, e.g.:
#
#   export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...   # or aws configure
#   terraform init && terraform plan                          # review first!
#
# Nothing here has been applied; `terraform plan` before any `apply`.

terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

variable "region" {
  description = "Cloud region for the VM"
  type        = string
  default     = "us-east-1"
}

variable "instance_type" {
  description = "VM size. t3.micro / t4g.micro sit in the AWS free tier; equivalent small sizes exist on every cloud."
  type        = string
  default     = "t3.micro"
}

variable "project_name" {
  description = "Name prefix for created resources"
  type        = string
  default     = "streaming-fraud-capstone"
}

provider "aws" {
  region = var.region
}

# Latest Ubuntu LTS AMI (looked up, never hard-coded).
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
}

resource "aws_security_group" "app" {
  name        = "${var.project_name}-sg"
  description = "SSH for admin; app ports stay closed by default"

  ingress {
    description = "SSH (restrict cidr_blocks to your own IP before applying)"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # TODO: tighten to your IP, e.g. ["203.0.113.10/32"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "app" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  vpc_security_group_ids = [aws_security_group.app.id]

  # Bootstrap: install Docker, clone the repo, bring the stack up.
  user_data = <<-EOT
    #!/bin/bash
    apt-get update && apt-get install -y docker.io docker-compose-plugin git
    # git clone <this-repo> /opt/streaming-fraud-capstone
    # cd /opt/streaming-fraud-capstone && docker compose up -d
  EOT

  tags = {
    Name    = var.project_name
    Project = "streaming-fraud-capstone"
  }
}

output "instance_public_ip" {
  description = "Public IP of the scoring VM"
  value       = aws_instance.app.public_ip
}
