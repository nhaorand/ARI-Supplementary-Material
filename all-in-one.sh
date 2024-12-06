cd ../; ./all-in-one.sh; cd ARI-Supplementary-Material 

rm -rf ./results
mkdir -p ./results

python3 -u runners/run-tests-sqlsolver.py -time -tests calcite -rounds 1 2>&1 | tee ./results/sqlsolver_calcite.out
python3 -u runners/run-tests-sqlsolver.py -time -tests spark -rounds 1 2>&1 | tee ./results/sqlsolver_spark.out
python3 -u runners/run-tests-sqlsolver.py -time -tests tpcc -rounds 1 2>&1 | tee ./results/sqlsolver_tpcc.out
python3 -u runners/run-tests-sqlsolver.py -time -tests tpch -rounds 1 2>&1 | tee ./results/sqlsolver_tpch.out
python3 runners/find-sqlsolver-rules.py > ./results/newrules.txt
python3 runners/find-wetune-rules.py > ./results/wetunerules.txt
python3 postprocess.py > ./results/reproduction.txt 2>&1
