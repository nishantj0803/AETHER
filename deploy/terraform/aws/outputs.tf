output "eks_cluster_endpoint" {
  description = "EKS control plane endpoint"
  value       = aws_eks_cluster.aether_eks.endpoint
}

output "eks_cluster_name" {
  description = "EKS cluster name"
  value       = aws_eks_cluster.aether_eks.name
}

output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.aether_vpc.id
}

output "private_subnet_ids" {
  description = "Private Subnet IDs for worker nodes and data plane"
  value       = aws_subnet.private_subnets[*].id
}

output "public_subnet_ids" {
  description = "Public Subnet IDs for external load balancers and NAT"
  value       = aws_subnet.public_subnets[*].id
}
