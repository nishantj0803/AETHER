output "gke_cluster_endpoint" {
  description = "GKE cluster API endpoint"
  value       = google_container_cluster.aether_gke.endpoint
}

output "gke_cluster_name" {
  description = "GKE cluster name"
  value       = google_container_cluster.aether_gke.name
}

output "network_name" {
  description = "VPC network name"
  value       = google_compute_network.aether_network.name
}
