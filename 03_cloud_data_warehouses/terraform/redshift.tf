resource "aws_redshift_cluster" "sparkify_redshift" {
    allow_version_upgrade               = true
    automated_snapshot_retention_period = 0
    availability_zone                   = "us-west-2d"
    cluster_identifier                  = "fozcodes-cloud-data-warehouse"
    cluster_parameter_group_name        = "default.redshift-1.0"
    cluster_revision_number             = "35649"
    cluster_security_groups             = []
    cluster_subnet_group_name           = "default"
    cluster_type                        = "multi-node"
    cluster_version                     = "1.0"
    database_name                       = "sparkify"
    encrypted                           = false
    enhanced_vpc_routing                = false
    iam_roles                           = [
        "arn:aws:iam::762323173043:role/service-role/AmazonRedshift-CommandsAccessRole-20220227T092018",
    ]
    master_username                     = var.redshift_user
    master_password                     = var.redshift_pwd
    node_type                           = "dc2.large"
    number_of_nodes                     = 1
    port                                = 5439
    preferred_maintenance_window        = "sun:07:30-sun:08:00"
    publicly_accessible                 = true
    skip_final_snapshot                 = true
    vpc_security_group_ids              = [
        "sg-0d68c184d9e3bfc7a",
    ]

    logging {
        enable = false
    }

    timeouts {}
}


