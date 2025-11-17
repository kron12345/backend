-/bin/sh

mkdir -p ./data_deu

python3 era_ops_export-V1.0.py  --country DEU \
                             --outfile ./data_deu/ops_DEU.csv \
                             --page-size 1500 --parallel 5 --timeout 120 --retries 7

python3 era_sols_export-v1.0.py \
              --country DEU --outfile ./data_deu/sols_DEU.csv \
              --sol-prefixes 0,1,2,3,4,5,6,7,8,9,A,B,C,D,E,F \
              --limit-sols 0 \
              --page-size 1500 --min-page-size 300 \
              --timeout 90 --retries 7 \
              --batch-endpoints 120 --min-batch-endpoints 40 \
              --batch-meta 80 --min-batch-meta 10 \
              --batch-opids 120 --min-batch-opids 40 \
              --batch-track-dirs 120 --min-batch-track-dirs 40 \
              --batch-track-prop 80 --min-batch-track-prop 30 \
              --batch-labels 20 --min-batch-labels 5 \
              --csv-bom --skip-on-timeout


python3 normalize_ids_op.py --in ./data_deu/ops_DEU.csv --out ./data_deu/NetNodeStandardMasterResource.csv --prefix DE --fillchar "0"
python3 normalize_ids_sol.py --in ./data_deu/sols_DEU.csv --out ./data_deu/NetEdgeStandardMasterResource.csv --prefix DE --fillchar "0"


python3 build_streckenkunde.py \
                             --sol ./data/NetEdgeStandardMasterResource.csv \
                             --op  ./data/NetNodeStandardMasterResource.csv \
                             --xml-out ./data/streckenkunde.xml \
                             --sol-out ./data/NetEdgeStandardMasterResource_with_qual.csv
