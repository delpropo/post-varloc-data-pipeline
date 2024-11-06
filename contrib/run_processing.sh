#!/bin/bash

# export PATH="/home/delpropo/miniconda3/etc/profile.d/conda.sh"
source /home/delpropo/.bashrc
export TMPDIR=/scratch/sooeunc_root/sooeunc0/delpropo

conda activate sgkit_env






# python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/tsv_to_zarr.py --input \
#  /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/9790-JD-P1.filter_NULL.fdr-controlled.tsv # --output 
  


# python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/eunc_data_transformation.py --input \
#  /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/post/9790-JD-P1.filter_NULL.fdr-controlled.zarr \
#  --output   20240312_9790-JD-P1 
 # python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/tsv_to_zarr.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/MS11114101.filter_NULL.fdr-controlled.tsv
 # python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/tsv_to_zarr.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/MS11118101.filter_NULL.fdr-controlled.tsv
 # python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/tsv_to_zarr.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/MS11130101.filter_NULL.fdr-controlled.tsv
 # python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/tsv_to_zarr.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/MS11144101.filter_NULL.fdr-controlled.tsv
 # python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/tsv_to_zarr.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/MS11146101.filter_NULL.fdr-controlled.tsv
 # python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/tsv_to_zarr.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/MS11147101.filter_NULL.fdr-controlled.tsv
 # python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/tsv_to_zarr.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/MS12115001.filter_NULL.fdr-controlled.tsv
 # python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/tsv_to_zarr.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/MS12213001.filter_NULL.fdr-controlled.tsv
 # python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/tsv_to_zarr.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/MS12216001.filter_NULL.fdr-controlled.tsv
 # python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/tsv_to_zarr.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/MS12226001.filter_NULL.fdr-controlled.tsv
 # python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/tsv_to_zarr.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/MS41206001.filter_NULL.fdr-controlled.tsv
 # python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/tsv_to_zarr.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/MS41214001.filter_NULL.fdr-controlled.tsv



 python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/post/MS11114101.filter_NULL.fdr-controlled.zarr --output 20240314_MS11114101
 python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/post/MS11118101.filter_NULL.fdr-controlled.zarr --output 20240314_MS11118101
 python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/post/MS11130101.filter_NULL.fdr-controlled.zarr --output 20240314_MS11130101
 python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/post/MS11144101.filter_NULL.fdr-controlled.zarr --output 20240314_MS11144101
 python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/post/MS11146101.filter_NULL.fdr-controlled.zarr --output 20240314_MS11146101
 python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/post/MS11147101.filter_NULL.fdr-controlled.zarr --output 20240314_MS11147101
 python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/post/MS12115001.filter_NULL.fdr-controlled.zarr --output 20240314_MS12115001
 python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/post/MS12213001.filter_NULL.fdr-controlled.zarr --output 20240314_MS12213001
 python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/post/MS12216001.filter_NULL.fdr-controlled.zarr --output 20240314_MS12216001
 python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/post/MS12226001.filter_NULL.fdr-controlled.zarr --output 20240314_MS12226001
 python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/post/MS41206001.filter_NULL.fdr-controlled.zarr --output 20240314_MS41206001
 python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/post/MS41214001.filter_NULL.fdr-controlled.zarr --output 20240314_MS41214001

