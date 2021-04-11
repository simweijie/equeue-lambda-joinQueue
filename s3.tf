terraform {
  backend "s3" {
    bucket = "nus-iss-equeue-terraform"
    key    = "lambda/joinQueue/tfstate"
    region = "us-east-1"
  }
}
