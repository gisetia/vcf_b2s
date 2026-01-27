


Tests folder structure
```
├── tests/
│   ├── run_tests.py        # The python script that runs the checks
│   │
│   ├── resources/          # Static files needed for testing
│   │   └── test_ref.fa     # A small "fake" reference genome 
│   │   └── test_ref.fa.fai # The index for the fake genome
│   │
│   ├── cases/              # Test Scenarios
│   │   ├── decompose/
│   │   │   ├── basic.input.vcf
│   │   │   └── basic.expected.vcf
│   │
│   └── integration/               
│   │   └── x.input.vcf
│   │   └── x.expected.vcf
│   │
│   └── temp/               # Git-ignored folder for actual outputs
│       └── (empty)
```