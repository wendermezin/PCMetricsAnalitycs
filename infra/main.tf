variable "minio_access_key" {
  type = string
}

variable "minio_secret_key" {
  type = string
}

provider "minio" {
  minio_endpoint = "localhost:9000"
  minio_access_key = var.minio_access_key
  minio_secret_key = var.minio_secret_key
  minio_api_version = "v4"
  insecure = true
}

resource "minio_bucket" "metrics_bucket" {
  bucket = "pcmetrics"
  acl    = "public-read-write"
}