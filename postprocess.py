import os

baseline = ["udp", "spes", "wetune"]
testset = ["calcite", "spark", "tpcc", "tpch"]
baseline_results = {}
sqlsolver_pass_cases = {}
sqlsolver_verification_times = {}

def analyze_results(benchmark):
    pass_cases = []
    verification_times = []
    lines = []
    with open("./results/sqlsolver_" + benchmark + ".out", "r") as f:
        lines = f.readlines()
    for line in lines:
        line = line.replace("\r", "").replace("\n", "").replace(" ", "")
        if (len(line) > 4) and (line.find("Case") == 0) and (line.find("NEQ") < 0):
            is_idx = line.find("is")
            case_id = int(line[4:is_idx])
            pass_cases.append(case_id)
            ms_idx = line.find("ms")
            eq_idx = line.find("EQ")
            verification_time = int(line[eq_idx+2:ms_idx])
            verification_times.append(verification_time)
    sqlsolver_pass_cases[benchmark] = pass_cases
    sqlsolver_verification_times[benchmark] = verification_times

def get_sqlsolver_verification_time(benchmark, int_id):
    pass_cases = sqlsolver_pass_cases[benchmark]
    verification_times = sqlsolver_verification_times[benchmark]
    for i in range(len(pass_cases)):
        if pass_cases[i] == int_id:
            return verification_times[i]
    return 0

def extract_rules_number(filename):
    lines = []
    with open(filename, "r") as f:
        lines = f.readlines()
    total_number = 0
    for line in lines:
        line = line.replace(" ", "").replace("\r", "").replace("\n", "")
        if len(line) > 4 and line.find("Rule") == 0 and line.find("NEQ") < 0:
            total_number += 1
    return total_number


def reproduce():
    print("1. The data in Table 1 of the paper:")
    pass_cases_cnt = len(sqlsolver_pass_cases["calcite"])
    print("SQLSolver can prove " + str(pass_cases_cnt) + " cases in Apache Calcite.")
    pass_cases_cnt = len(sqlsolver_pass_cases["spark"])
    print("SQLSolver can prove " + str(pass_cases_cnt) + " cases in Spark SQL.")

    print("\n\n2. The data in Table 5 of the paper:")
    for verifier in baseline:
        if verifier == "sqlsolver":
            continue
        unsupported_case_number = 0;
        for benchmark in testset:
            sqlsolver_pass_number = len(sqlsolver_pass_cases[benchmark])
            baseline_key = verifier+"-"+benchmark
            baseline_pass_number = len(baseline_results[verifier + "-" + benchmark])
            if verifier == "udp" and benchmark == "calcite":
                baseline_pass_number = 33
            unsupported_case_number += sqlsolver_pass_number - baseline_pass_number
        print("Unsupported cases of " + verifier + " is " + str(unsupported_case_number))

    print("\n\n3. The data in Table 6 of the paper:")
    print("Note that we only show the verification time of SQLSolver here for simplicity because the verification time of baselines has been shown in the SIGMOD ARI submission.")
    for verifier in baseline:
        for benchmark in testset:
            baseline_pass_cases = baseline_results[verifier + "-" + benchmark]
            baseline_pass_cases_cnt = len(baseline_pass_cases)
            if baseline_pass_cases_cnt == 0:
                continue
            total_time = 0
            for int_id in baseline_pass_cases:
                total_time += get_sqlsolver_verification_time(benchmark, int_id)
            print("Compared with " + verifier + " on the testset of " + benchmark +", the verification time of SQLSolver is " + str(int(total_time/baseline_pass_cases_cnt)))

    print("\n\n4. The data at the beginning of the first paragraph in Section 6.2:")
    baseline_pass_cases_number = 0
    for benchmark in testset:
        pass_cases_set = []
        for verifier in baseline:
            baseline_pass_cases = baseline_results[verifier+"-"+benchmark]
            for case_id in baseline_pass_cases:
                if not (case_id in pass_cases_set):
                    pass_cases_set.append(case_id)
        baseline_pass_cases_number += len(pass_cases_set)
    print("Among all 400 equivalent query pairs, UDP, SPES, and WeTune are able to prove the equivalence of " + str(baseline_pass_cases_number + 1) + " pairs in total.")
    sqlsolver_pass_number = 0
    for benchmark in testset:
        sqlsolver_pass_number += len(sqlsolver_pass_cases[benchmark])
    print("SQLSolver can prove "+str(sqlsolver_pass_number)+" pairs, "+str(sqlsolver_pass_number-baseline_pass_cases_number-1)+" of which cannot be proved by any of these existing provers.")

    print("\n\n5. The data in the last sentence of Section 1:")
    new_rules_number = extract_rules_number("./results/newrules.txt")
    print("When using SQLSolver to discover SQL rewrite rules, we find "+str(new_rules_number)+" new rewrite rules beyond the 35 rules found by using the existing solver in WeTune.")

    print("\n\n6. The data in Section 6.3:")
    wetune_rules_number = extract_rules_number("./results/wetunerules.txt")
    print("Our integration of SQLSolver reveals all "+str(wetune_rules_number)+" useful rules previously found by WeTune.")


def init_baseline_results():
    lines = []
    with open("baseline.txt", "r") as f:
        lines = f.readlines()
    for line in lines:
        line = line.replace(" ", "").replace("\r", "").replace("\n", "")
        items = line.split("=")
        key = items[0]
        ids_str = items[1]
        ids = ids_str[1:-1].split(",")
        ids_int = []
        for id in ids:
            if len(id) > 0:
                ids_int.append(int(id))
        baseline_results[key] = ids_int

def postprocess():
    init_baseline_results()
    for benchmark in testset:
        analyze_results(benchmark)
    reproduce()

print("Some data in the paper have been shown in the previous SIGMOD ARI submission and are not affected by different versions of SQLSolver source code. Therefore, this script will not show these data.")
print("\n")
postprocess()
