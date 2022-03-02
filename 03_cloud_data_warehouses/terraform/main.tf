provider "aws" {
  version = "~> 2.0"
  region  = var.aws_working_region
  profile = var.aws_profile
  token   = var.aws_session_token
}

