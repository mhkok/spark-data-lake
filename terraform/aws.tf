terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
      region  = "eu-west-1"
      profile = "matthijs.kok"
    }
  }
}

provider "aws" {
    region = "eu-west-1"
}