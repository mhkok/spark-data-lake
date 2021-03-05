

module "emr_cluster" {
  source                                         = "cloudposse/emr-cluster/aws"
  version                                        = "0.19.1"
  namespace                                      = var.namespace
  stage                                          = var.stage
  name                                           = var.name
  master_allowed_security_groups                 = [module.spark_sg.this_security_group_id]
  slave_allowed_security_groups                  = [module.spark_sg.this_security_group_id]
  region                                         = var.region
  vpc_id                                         = var.vpc_id
  subnet_id                                      = var.subnet_ids[0]
  route_table_id                                 = var.route_table_id[0]
  subnet_type                                    = "public"
  ebs_root_volume_size                           = var.ebs_root_volume_size
  visible_to_all_users                           = var.visible_to_all_users
  release_label                                  = var.release_label
  applications                                   = var.applications
  configurations_json                            = var.configurations_json
  core_instance_group_instance_type              = var.core_instance_group_instance_type
  core_instance_group_instance_count             = var.core_instance_group_instance_count
  core_instance_group_ebs_size                   = var.core_instance_group_ebs_size
  core_instance_group_ebs_type                   = var.core_instance_group_ebs_type
  core_instance_group_ebs_volumes_per_instance   = var.core_instance_group_ebs_volumes_per_instance
  master_instance_group_instance_type            = var.master_instance_group_instance_type
  master_instance_group_instance_count           = var.master_instance_group_instance_count
  master_instance_group_ebs_size                 = var.master_instance_group_ebs_size
  master_instance_group_ebs_type                 = var.master_instance_group_ebs_type
  master_instance_group_ebs_volumes_per_instance = var.master_instance_group_ebs_volumes_per_instance
  create_task_instance_group                     = var.create_task_instance_group
  log_uri                                        = format("s3n://%s/", module.s3_log_storage.bucket_id)
  key_name                                       = var.key_name
}

module "s3_log_storage" {
  source  = "cloudposse/s3-log-storage/aws"
  version = "0.14.0"

  attributes    = ["spark-logs-mk"]
  force_destroy = true

}

module "spark_sg" {
  source = "terraform-aws-modules/security-group/aws"

  name        = "spark-datalake-sg"
  description = "Security group for spark"
  vpc_id      = var.vpc_id

  ingress_cidr_blocks      = var.ingress_with_cidr_blocks
  ingress_rules            = ["https-443-tcp"]
}
