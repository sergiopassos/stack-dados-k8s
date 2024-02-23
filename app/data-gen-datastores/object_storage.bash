cd "/Users/luanmorenomaciel/GitHub/ws-stack-dados-k8s/app/data-gen-datastores/"

for i in {1..1000}
do
   python cli.py all parquet
done
