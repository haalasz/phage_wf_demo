rule merge_hq_pharokka:
    input:
        hq   = "results/hq/{accession}_hq.tsv",
        pharokka_dir = "results/pharokka/{accession}"
    output:
        tsv = "results/merged/{accession}_merged.tsv"
    conda:
        "../envs/merge.yaml"
    log:
        "logs/merge/{accession}.log"
    script:
        "../scripts/merge_hq_pharokka.py"