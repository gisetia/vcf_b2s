# vcf_b2s

Spark-based VCF normalization + annotation pipeline.

## Quickstart (uv)
1) Create a virtualenv:
   - `uv venv`
   - `source .venv/bin/activate`
2) Install Python dependencies:
   - `uv pip install -r requirements.txt`

## Prerequisites
- Java 11 (Spark). `spark_setup.py` will use `./.jdk` if present, otherwise `JAVA_HOME`.
- `bcftools` available on `PATH`.
- Ensembl VEP installed; update paths in `VCF_b2s_vanilla_spark.py`:
  - `REFERENCE_GENOME`
  - `VEP_PATH`
  - `VEP_CACHE`

