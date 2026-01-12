# %%
import os
import glow
from spark_setup import get_spark_session, register_glow

spark = register_glow(get_spark_session())

REFERENCE_GENOME = '/Users/guizela/data/references/hg38.fa'
INPUT_DIR = 'files/input_vcf'
TMP_DIR = 'files/tmp_glow'
OUTPUT_DIR = 'files/output_glow'

VEP_PATH = '/Users/guizela/tools/ensembl-vep'
VEP_CACHE = '/Users/guizela/data/references/vep_cache'

VCF_TYPE = "mutect2"
VCF_FIELD_MAPS = {
    # Examples; extend with the conventions you see for each VCF type.
    "mutect2": {
        "vaf": {"source": "FORMAT", "key": "AF"},
        "depth": {"source": "FORMAT", "key": "DP"},
    },
    "foundation_one": {
        "vaf": {"source": "INFO", "key": "AF"},
        "depth": {"source": "INFO", "key": "DP"},
    },
}

#%%

if not os.path.exists(TMP_DIR):
    os.makedirs(TMP_DIR)
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
if not os.path.exists(os.path.join(OUTPUT_DIR, 'variants')):
    os.makedirs(os.path.join(OUTPUT_DIR, 'variants'))
if not os.path.exists(os.path.join(OUTPUT_DIR, 'calls')):
    os.makedirs(os.path.join(OUTPUT_DIR, 'calls'))

vcf_files = [os.path.join(INPUT_DIR, f) for f in os.listdir(INPUT_DIR)]

vcf_file = vcf_files[0]

df = spark.read.format("vcf").load(vcf_file)
split_variants_df = glow.transform("split_multiallelics", df)
df_normalized = glow.transform("normalize_variants", split_variants_df, 
                               reference_genome_path=REFERENCE_GENOME,
                               replace_columns=True)
df_normalized.show()
df_normalized.write.format("vcf").save(os.path.join(TMP_DIR, os.path.basename(vcf_file).replace('.vcf', '.norm.vcf')))

#%%

import json
scriptFile = f"""#!/bin/sh
set -e

{VEP_PATH}/vep \
--cache \
--dir_cache {VEP_CACHE} \
--offline \
--fasta {REFERENCE_GENOME} \
--format vcf \
--force_overwrite \
--output_file STDOUT \
"""
 
cmd = json.dumps(["bash", "-c", scriptFile])

output_df = glow.transform("pipe", 
                           df_normalized, 
                           cmd=cmd, 
                           input_formatter='vcf', 
                           in_vcf_header='infer',
                           output_formatter='vcf'
                          )
output_df.show()

# %%
