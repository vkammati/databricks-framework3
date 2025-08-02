"""
A service principal is used to deploy in dev Databricks entities necessary for the pipeline to run properly.
As a result, only the service principal (and the workspace admins) can access those entities.
In dev, we want the developer to have access to them and interact with them (trigger, update, etc.)

This script sets open permissions for all those entities:
- the Repos
- the DLT pipeline itself
- the workflow that wraps the DLT pipeline
- the workflow that registers tables to Unity Catalog
- the cluster used to register tables to Unity Catalog
"""

import argparse

from ci_cd_helpers.auth import get_dbx_http_header
from ci_cd_helpers.azure import generate_spn_ad_token
from ci_cd_helpers.helpers import (
    get_cluster_name,
    get_dlt_pipeline_name,
    get_register_table_uc_workflow_name,
    get_repos_parent_folder_path,
    get_repos_path,
    get_unique_repo_branch_id,
    get_wrapping_workflow_name
)
from ci_cd_helpers.repos import get_repos_id, set_repos_open_permissions
from ci_cd_helpers.clusters import get_cluster_id, set_cluster_open_permissions
from ci_cd_helpers.pipelines import get_pipeline_id, set_pipeline_open_permissions
from ci_cd_helpers.workflows import get_job_id, set_workflow_open_permissions
from ci_cd_helpers.workspace import get_folder_id, set_folder_open_permissions

parser = argparse.ArgumentParser(description="Create all Databricks entites for the pipeline.")

parser.add_argument(
    "--host",
    help="Databricks host.",
    type=str,
)

parser.add_argument(
    "--tenant-id",
    help="Azure tenant id.",
    type=str,
)

parser.add_argument(
    "--spn-client-id",
    help="Service principal client id used to deploy code.",
    type=str,
)

parser.add_argument(
    "--spn-client-secret",
    help="Service principal client secret used to deploy code.",
    type=str,
)

parser.add_argument(
    "--repository",
    help="GitHub repository (with owner).",
    type=str,
)

parser.add_argument(
    "--branch-name",
    help="Name of the branch in GitHub",
    type=str,
)

parser.add_argument(
    "--env",
    help="Databricks/GitHub environment to use.",
    choices=["dev", "tst", "pre", "prd"],
    type=str,
)

parser.add_argument(
    "--deployment-type",
    help="Deployment type, either 'manual' or 'automatic'. Taken into account only when env=dev.",
    choices=["manual", "automatic"],
    type=str,
)

parser.add_argument(
    "--integration-test",
    help="Integration test type, either 'true' or 'false'. Taken into account only when env=dev.",
    choices=["true", "false"],
    type=str,
)

args = parser.parse_args()

'''if args.env == "dev" and not args.deployment_type:
    raise ValueError("When env=dev, parameter 'type' must be either 'manual' or 'automatic', not None.")'''
    
if args.integration_test == "true":
    dv_suffix =  "DV-" +args.deployment_type 
    uom_suffix =  "UOM-" +args.deployment_type 
    uom_demo_suffix =  "UOM-DEMO-" +args.deployment_type 
else:
    dv_suffix =  "-DV"
    uom_suffix =  "-UOM"
    uom_demo_suffix =  "-UOM-DEMO"

# necessary to call the Databricks REST API
ad_token = generate_spn_ad_token(args.tenant_id, args.spn_client_id, args.spn_client_secret)
http_header = get_dbx_http_header(ad_token)

# Repos parent folder
repos_parent_folder_path = get_repos_parent_folder_path(args.repository)
repos_parent_folder_id = get_folder_id(repos_parent_folder_path, args.host, http_header)
set_folder_open_permissions(repos_parent_folder_id, args.host, http_header)

# Repos folder
if args.integration_test == "true":
    repos_path = get_repos_path(args.repository, args.branch_name, args.deployment_type)
else:    
    repos_path = repos_parent_folder_path + "/datta_tech_eh"
repos_id = get_repos_id(repos_path, args.host, http_header)
set_repos_open_permissions(repos_id, args.host, http_header)

# DLT pipeline
# if args.integration_test == "true":
#     pipeline_name = get_dlt_pipeline_name(args.repository, args.branch_name, args.deployment_type)
# else:    
#     #pipeline_name = "sede-x-datta-copa-dlt"
#     pipeline_name = get_dlt_pipeline_name(args.repository)
# pipeline_id = get_pipeline_id(pipeline_name, args.host, http_header)
# set_pipeline_open_permissions(pipeline_id, args.host, http_header)

# Wrapping workflow
if args.integration_test == "true":

    workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, args.deployment_type)
else:
    #workflow_name = "sede-x-datta-copa-workflow"
    workflow_name = get_wrapping_workflow_name(args.repository)
workflow_id = get_job_id(workflow_name, args.host, http_header)
set_workflow_open_permissions(workflow_id, args.host, http_header)

# dv wrapping workflow
if args.integration_test == "true":
    dv_workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, dv_suffix)
else:
    dv_workflow_full_suffix = args.repository+dv_suffix
    dv_workflow_name = get_wrapping_workflow_name(dv_workflow_full_suffix)
dv_workflow_id = get_job_id(dv_workflow_name, args.host, http_header)
set_workflow_open_permissions(dv_workflow_id, args.host, http_header)

# uom wrapping workflow
if args.integration_test == "true":
    uom_workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, uom_suffix)
else:
    uom_workflow_full_suffix = args.repository+uom_suffix
    uom_workflow_name = get_wrapping_workflow_name(uom_workflow_full_suffix)
uom_workflow_id = get_job_id(uom_workflow_name, args.host, http_header)
set_workflow_open_permissions(uom_workflow_id, args.host, http_header)

# uom demo wrapping workflow
if args.integration_test == "true":
    uom_demo_workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, uom_demo_suffix)
else:
    uom_demo_workflow_full_suffix = args.repository+uom_demo_suffix
    uom_demo_workflow_name = get_wrapping_workflow_name(uom_demo_workflow_full_suffix)
uom_demo_workflow_id = get_job_id(uom_demo_workflow_name, args.host, http_header)
set_workflow_open_permissions(uom_demo_workflow_id, args.host, http_header)

# UC workflow
# if args.integration_test == "true":
#     register_table_uc_name = get_register_table_uc_workflow_name(args.repository, args.branch_name, args.deployment_type)
# else:
#     #register_table_uc_name = "sede-x-datta-copa-uc"
#     register_table_uc_name = get_register_table_uc_workflow_name(args.repository)
# register_table_uc_id = get_job_id(register_table_uc_name, args.host, http_header)
# set_workflow_open_permissions(register_table_uc_id, args.host, http_header)

# cluster to register tables to UC
if args.integration_test == "true":
    cluster_name = get_cluster_name(args.repository, args.branch_name, args.deployment_type)
    cluster_id = get_cluster_id(cluster_name, args.host, http_header)
    set_cluster_open_permissions(cluster_id, args.host, http_header)
# else:
#     #cluster_name = "sede-x-datta-fcb-cluster"
#     cluster_name = get_cluster_name(args.repository)
# cluster_id = get_cluster_id(cluster_name, args.host, http_header)
# set_cluster_open_permissions(cluster_id, args.host, http_header)
