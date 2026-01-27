# %%
import re
import os
import subprocess
import pandas as pd
from io import StringIO
from spark_setup import get_spark_session

REFERENCE_GENOME = '/Users/guizela/data/references/hg38.fa'
INPUT_DIR = 'files/input_vcf'
TMP_DIR = 'files/tmp'
OUTPUT_DIR = 'files/output'

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

spark = get_spark_session()
sc = spark.sparkContext

# %%

vcf_files = [os.path.join(INPUT_DIR, f) for f in os.listdir(INPUT_DIR)]
rdd = sc.parallelize(vcf_files, numSlices=1)

if not os.path.exists(TMP_DIR):
    os.makedirs(TMP_DIR)
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
if not os.path.exists(os.path.join(OUTPUT_DIR, 'variants')):
    os.makedirs(os.path.join(OUTPUT_DIR, 'variants'))
if not os.path.exists(os.path.join(OUTPUT_DIR, 'calls')):
    os.makedirs(os.path.join(OUTPUT_DIR, 'calls'))


def annotate_variants(norm_vcf):

    try:
        output_tsv = os.path.join(OUTPUT_DIR, f"variants/{os.path.basename(norm_vcf).replace('.norm.vcf', '.tsv')}")
        cmd = (f"{VEP_PATH}/vep --cache --dir_cache {VEP_CACHE} --offline \
            --tab --show_ref_allele --uploaded_allele --variant_class \
            --fasta {REFERENCE_GENOME} --hgvsg --symbol --canonical --hgvs --uniprot --flag_pick --mane \
            --force_overwrite -i {norm_vcf} -o {output_tsv}")

        print(f"Running command: \n{cmd}")
        result = subprocess.run(cmd, shell=True, check=True)
        print(f"-- stdout:\n{result.stdout}\n-- stderr:\n{result.stderr}")

    except subprocess.CalledProcessError as e:
        print(f"Error annotating {norm_vcf}: {e}")

    return None


def build_bcftools_format(vcf_type):
    info_keys = []
    format_keys = []
    for spec in VCF_FIELD_MAPS[vcf_type].values():
        if spec["source"] == "INFO":
            info_keys.append(spec["key"])
        elif spec["source"] == "FORMAT":
            format_keys.append(spec["key"])
        else:
            raise ValueError(f"Unknown source: {spec['source']}")

    parts = ["%ID"]
    parts += [f"%INFO/{k}" for k in info_keys]

    if format_keys:
        fmt = [f"%{k}" for k in format_keys]
        parts.append("[" + "\\t".join(fmt) + "\\t]")

    return "\\t".join(parts) + "\\n"


def parse_calls(VCF_TYPE, norm_vcf):

    cmd = f"""bcftools query -H -f '{build_bcftools_format(VCF_TYPE)}' {norm_vcf}"""

    print(f"Running command: \n{cmd}")
    result = subprocess.run(cmd, shell=True, check=True,
                            capture_output=True, text=True)
    print(f"-- stdout:\n{result.stdout}\n-- stderr:\n{result.stderr}")

    calls_df = pd.read_csv(StringIO(result.stdout), sep="\t")
    calls_df = calls_df.drop(columns=[c for c in calls_df.columns if c.startswith("Unnamed")])
    calls_df.columns = [clean_col_name(c) for c in calls_df.columns]
    calls_df.to_csv(os.path.join(OUTPUT_DIR, f"calls/{os.path.basename(norm_vcf).replace('.vcf', '.tsv')}"), sep="\t", index=False)

    return calls_df


def clean_col_name(c):
    return re.sub(r"^#?\[\d+\]", "", c).strip()


def normalize_vcf(vcf_path):

    clean_path = os.path.join(TMP_DIR, os.path.basename(
        vcf_path).replace(".vcf", ".norm.vcf"))
    cmd = (f"bcftools norm --multiallelics - \
           --rm-dup exact \
           --fasta-ref {REFERENCE_GENOME} \
           --check-ref e \
           --sort lex\
           --output-type u {vcf_path} \
           | bcftools annotate --set-id '%CHROM:%POS:%REF:%ALT' \
           --output-type v -o {clean_path}")
    print(f"Running command: \n{cmd}")
    try:
        result = subprocess.run(cmd, shell=True, check=True,
                                capture_output=True, text=True)
        print(f"-- stdout:\n{result.stdout}\n-- stderr:\n{result.stderr}")

    except subprocess.CalledProcessError as e:
        print(f"Error processing {vcf_path}: {e}")
        return None

    return clean_path


def process_partition(iterator):

    results = []
    for vcf_path in iterator:

        # # Normalize, decompose multiallelics, check reference, and set variant IDs with bcftools
        clean_path = normalize_vcf(vcf_path)

        # Annotate variants with VEP
        annotate_variants(clean_path)

        # Parse variant calls into tabular format
        calls = parse_calls(VCF_TYPE, clean_path)
 
    return calls

rdd.mapPartitions(process_partition).collect()

# %%
