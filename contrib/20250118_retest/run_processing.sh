#!/bin/bash

# export PATH="/home/delpropo/miniconda3/etc/profile.d/conda.sh"
source /home/delpropo/.bashrc
export TMPDIR=/scratch/sooeunc_root/sooeunc0/delpropo

conda activate sgkit_env

# tsv_to_zarr_output.py first on 1/9/24
# python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/tsv_to_zarr_output.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/9790-JD-10.filter_NULL.variants.fdr-controlled.tsv --output /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/20250109/
# python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/tsv_to_zarr_output.py --input /nfs/turbo/umms-mblabns/varlociraptor/results/tables/EP177-group.filter_test_candidates_af_filter.variants.fdr-controlled.tsv --output /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/20250109/
# python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/20250109/9790-JD-10.filter_NULL.variants.fdr-controlled.zarr
# python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/20250109/EP177-group.filter_test_candidates_af_filter.variants.fdr-controlled.zarr


##  large scale testing 1/10/25

# python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/tsv_to_zarr_output.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/9790-JD-10.filter_NULL.variants.fdr-controlled.tsv  --output /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/20250110/
# python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/tsv_to_zarr_output.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/9790-JD-11.filter_NULL.variants.fdr-controlled.tsv     --output /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/20250110/
# python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/tsv_to_zarr_output.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/9790-JD-12.filter_NULL.variants.fdr-controlled.tsv  --output /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/20250110/
# python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/tsv_to_zarr_output.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/9790-JD-1.filter_NULL.variants.fdr-controlled.tsv  --output /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/20250110/
# python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/tsv_to_zarr_output.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/9790-JD-2.filter_NULL.variants.fdr-controlled.tsv  --output /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/20250110/
# python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/tsv_to_zarr_output.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/9790-JD-3.filter_NULL.variants.fdr-controlled.tsv  --output /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/20250110/
# python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/tsv_to_zarr_output.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/9790-JD-4.filter_NULL.variants.fdr-controlled.tsv  --output /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/20250110/
# python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/tsv_to_zarr_output.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/9790-JD-5.filter_NULL.variants.fdr-controlled.tsv  --output /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/20250110/
# python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/tsv_to_zarr_output.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/9790-JD-6.filter_NULL.variants.fdr-controlled.tsv  --output /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/20250110/
# python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/tsv_to_zarr_output.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/9790-JD-7.filter_NULL.variants.fdr-controlled.tsv  --output /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/20250110/
# python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/tsv_to_zarr_output.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/9790-JD-8.filter_NULL.variants.fdr-controlled.tsv  --output /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/20250110/
# python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/tsv_to_zarr_output.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/9790-JD-9.filter_NULL.variants.fdr-controlled.tsv  --output /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/20250110/

python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/20250110/9790-JD-10.filter_NULL.variants.fdr-controlled.zarr
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/20250110/9790-JD-11.filter_NULL.variants.fdr-controlled.zarr
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/20250110/9790-JD-12.filter_NULL.variants.fdr-controlled.zarr
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/20250110/9790-JD-1.filter_NULL.variants.fdr-controlled.zarr
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/20250110/9790-JD-2.filter_NULL.variants.fdr-controlled.zarr
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/20250110/9790-JD-3.filter_NULL.variants.fdr-controlled.zarr
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/20250110/9790-JD-4.filter_NULL.variants.fdr-controlled.zarr
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/20250110/9790-JD-5.filter_NULL.variants.fdr-controlled.zarr
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/20250110/9790-JD-6.filter_NULL.variants.fdr-controlled.zarr
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/20250110/9790-JD-7.filter_NULL.variants.fdr-controlled.zarr
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/20250110/9790-JD-8.filter_NULL.variants.fdr-controlled.zarr
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/20250110/9790-JD-9.filter_NULL.variants.fdr-controlled.zarr


python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/tsv_to_zarr_output.py --input /nfs/turbo/umms-mblabns/post_varloc_processing/data_transfer/EP138.filter_NULL_shared.fdr-controlled.tsv --output /nfs/turbo/umms-mblabns/post_varloc_processing/temp/20250110/
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/tsv_to_zarr_output.py --input /nfs/turbo/umms-mblabns/post_varloc_processing/data_transfer/EP144.filter_NULL_shared.fdr-controlled.tsv   --output /nfs/turbo/umms-mblabns/post_varloc_processing/temp/20250110/
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/tsv_to_zarr_output.py --input /nfs/turbo/umms-mblabns/post_varloc_processing/data_transfer/EP145.filter_NULL_denovo.fdr-controlled.tsv          --output /nfs/turbo/umms-mblabns/post_varloc_processing/temp/20250110/
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/tsv_to_zarr_output.py --input /nfs/turbo/umms-mblabns/post_varloc_processing/data_transfer/EP145.filter_NULL_inherit.fdr-controlled.tsv  --output /nfs/turbo/umms-mblabns/post_varloc_processing/temp/20250110/
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/tsv_to_zarr_output.py --input /nfs/turbo/umms-mblabns/post_varloc_processing/data_transfer/EP149.filter_NULL_shared.fdr-controlled.tsv  --output /nfs/turbo/umms-mblabns/post_varloc_processing/temp/20250110/
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/tsv_to_zarr_output.py --input /nfs/turbo/umms-mblabns/post_varloc_processing/data_transfer/EP162.filter_NULL_denovo.fdr-controlled.tsv  --output /nfs/turbo/umms-mblabns/post_varloc_processing/temp/20250110/
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/tsv_to_zarr_output.py --input /nfs/turbo/umms-mblabns/post_varloc_processing/data_transfer/EP162.filter_NULL_inherit.fdr-controlled.tsv  --output /nfs/turbo/umms-mblabns/post_varloc_processing/temp/20250110/
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/tsv_to_zarr_output.py --input /nfs/turbo/umms-mblabns/post_varloc_processing/data_transfer/EP163.filter_NULL_shared.fdr-controlled.tsv  --output /nfs/turbo/umms-mblabns/post_varloc_processing/temp/20250110/
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/tsv_to_zarr_output.py --input /nfs/turbo/umms-mblabns/post_varloc_processing/data_transfer/EP164.filter_NULL_shared.fdr-controlled.tsv  --output /nfs/turbo/umms-mblabns/post_varloc_processing/temp/20250110/
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/tsv_to_zarr_output.py --input /nfs/turbo/umms-mblabns/post_varloc_processing/data_transfer/EP166.filter_NULL_shared.fdr-controlled.tsv  --output /nfs/turbo/umms-mblabns/post_varloc_processing/temp/20250110/
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/tsv_to_zarr_output.py --input /nfs/turbo/umms-mblabns/post_varloc_processing/data_transfer/EP173.filter_NULL_EP173_denovo.fdr-controlled.tsv  --output /nfs/turbo/umms-mblabns/post_varloc_processing/temp/20250110/
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/tsv_to_zarr_output.py --input /nfs/turbo/umms-mblabns/post_varloc_processing/data_transfer/EP173.filter_NULL_EP173_inherit.fdr-controlled.tsv  --output /nfs/turbo/umms-mblabns/post_varloc_processing/temp/20250110/
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/tsv_to_zarr_output.py --input /nfs/turbo/umms-mblabns/post_varloc_processing/data_transfer/EP176.filter_NULL_shared.fdr-controlled.tsv  --output /nfs/turbo/umms-mblabns/post_varloc_processing/temp/20250110/

python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/data_transformation.py  --input /nfs/turbo/umms-mblabns/post_varloc_processing/temp/20250110/EP138.filter_NULL_shared.fdr-controlled.zarr
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/data_transformation.py  --input /nfs/turbo/umms-mblabns/post_varloc_processing/temp/20250110/EP144.filter_NULL_shared.fdr-controlled.zarr
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/data_transformation.py  --input /nfs/turbo/umms-mblabns/post_varloc_processing/temp/20250110/EP145.filter_NULL_denovo.fdr-controlled.zarr
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/data_transformation.py  --input /nfs/turbo/umms-mblabns/post_varloc_processing/temp/20250110/EP145.filter_NULL_inherit.fdr-controlled.zarr
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/data_transformation.py  --input /nfs/turbo/umms-mblabns/post_varloc_processing/temp/20250110/EP149.filter_NULL_shared.fdr-controlled.zarr
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/data_transformation.py  --input /nfs/turbo/umms-mblabns/post_varloc_processing/temp/20250110/EP162.filter_NULL_denovo.fdr-controlled.zarr
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/data_transformation.py  --input /nfs/turbo/umms-mblabns/post_varloc_processing/temp/20250110/EP162.filter_NULL_inherit.fdr-controlled.zarr
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/data_transformation.py  --input /nfs/turbo/umms-mblabns/post_varloc_processing/temp/20250110/EP163.filter_NULL_shared.fdr-controlled.zarr
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/data_transformation.py  --input /nfs/turbo/umms-mblabns/post_varloc_processing/temp/20250110/EP164.filter_NULL_shared.fdr-controlled.zarr
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/data_transformation.py  --input /nfs/turbo/umms-mblabns/post_varloc_processing/temp/20250110/EP166.filter_NULL_shared.fdr-controlled.zarr
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/data_transformation.py  --input /nfs/turbo/umms-mblabns/post_varloc_processing/temp/20250110/EP173.filter_NULL_EP173_denovo.fdr-controlled.zarr
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/data_transformation.py  --input /nfs/turbo/umms-mblabns/post_varloc_processing/temp/20250110/EP173.filter_NULL_EP173_inherit.fdr-controlled.zarr
python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/data_transformation.py  --input /nfs/turbo/umms-mblabns/post_varloc_processing/temp/20250110/EP176.filter_NULL_shared.fdr-controlled.zarr








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


# Attempted to run this on 1/8/25.
 # python /home/delpropo/github/post-varloc-data-pipeline/post_varloc_data_pipeline/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/post/MS11114101.filter_NULL.fdr-controlled.zarr --output 20240314_MS11114101
 # python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/post/MS11130101.filter_NULL.fdr-controlled.zarr --output 20240314_MS11130101
 # python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/post/MS11144101.filter_NULL.fdr-controlled.zarr --output 20240314_MS11144101
 # python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/post/MS11146101.filter_NULL.fdr-controlled.zarr --output 20240314_MS11146101
 # python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/post/MS11118101.filter_NULL.fdr-controlled.zarr --output 20240314_MS11118101
 # python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/post/MS11147101.filter_NULL.fdr-controlled.zarr --output 20240314_MS11147101
 # python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/post/MS12115001.filter_NULL.fdr-controlled.zarr --output 20240314_MS12115001
 # python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/post/MS12213001.filter_NULL.fdr-controlled.zarr --output 20240314_MS12213001
 # python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/post/MS12216001.filter_NULL.fdr-controlled.zarr --output 20240314_MS12216001
 # python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/post/MS12226001.filter_NULL.fdr-controlled.zarr --output 20240314_MS12226001
 # python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/post/MS41206001.filter_NULL.fdr-controlled.zarr --output 20240314_MS41206001
 # python /home/delpropo/github/Umich_HPC/varlociraptor/post/scripts/eunc_data_transformation.py --input /nfs/turbo/umms-sooeunc/9790-JD/analysis/results/tables/post/MS41214001.filter_NULL.fdr-controlled.zarr --output 20240314_MS41214001

