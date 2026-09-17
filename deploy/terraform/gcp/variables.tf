variable "project_id" {
  description = "GCP Project ID"
  type        = string
  default     = "aether-production-cloud"
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}

variable "cluster_name" {
  description = "GKE cluster name"
  type        = string
  default     = "aether-prod-gke"
}
