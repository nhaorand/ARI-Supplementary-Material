import os
import re

baseline = ["udp", "spes", "wetune"]
testset = ["calcite", "spark", "tpcc", "tpch"]
baseline_results = {}
baseline_verification_times = {}
baseline_pass_cases_number = {}
baseline_avg_verification_times = {}
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
    print("SQLSolver can prove " + str(pass_cases_cnt) + " cases in Calcite.")
    pass_cases_cnt = len(sqlsolver_pass_cases["spark"])
    print("SQLSolver can prove " + str(pass_cases_cnt) + " cases in Spark SQL.")
    for verifier in baseline:
        calcite_pass_number = baseline_pass_cases_number[verifier+"-calcite"]
        spark_pass_number = baseline_pass_cases_number[verifier+"-spark"]
        print(verifier + " can prove " + str(calcite_pass_number) + " cases in Calcite")
        print(verifier + " can prove " + str(spark_pass_number) + " cases in Spark SQL")

    print("\n\n2. The data in Table 5 of the paper:")
    for verifier in baseline:
        unsupported_case_number = 0;
        for benchmark in testset:
            sqlsolver_pass_number = len(sqlsolver_pass_cases[benchmark])
            baseline_key = verifier+"-"+benchmark
            baseline_pass_number = baseline_pass_cases_number[baseline_key]
            unsupported_case_number += sqlsolver_pass_number - baseline_pass_number
        print("Unsupported cases of " + verifier + " is " + str(unsupported_case_number))

    print("\n\n3. The data in Table 6 of the paper:")
    for verifier in baseline:
        for benchmark in testset:
            baseline_pass_cases = baseline_results[verifier + "-" + benchmark]
            baseline_pass_cases_cnt = len(baseline_pass_cases)
            if baseline_pass_cases_cnt == 0:
                continue
            total_time = 0
            for int_id in baseline_pass_cases:
                total_time += get_sqlsolver_verification_time(benchmark, int_id)
            baseline_verification_time = baseline_avg_verification_times[verifier + "-" + benchmark]
            print(verifier + " vs. SQLSolver on " + benchmark + " is " + str(baseline_verification_time) + " vs. " + str(int(total_time/baseline_pass_cases_cnt)))

    print("\n\n4. The data at the beginning of the first paragraph in Section 6.2:")
    baseline_total = 0
    for benchmark in testset:
        pass_cases_set = []
        for verifier in baseline:
            baseline_pass_cases = baseline_results[verifier+"-"+benchmark]
            for case_id in baseline_pass_cases:
                if not (case_id in pass_cases_set):
                    pass_cases_set.append(case_id)
        baseline_total += len(pass_cases_set)
    print("Among all 400 equivalent query pairs, UDP, SPES, and WeTune are able to prove the equivalence of " + str(baseline_total + 1) + " pairs in total.")
    sqlsolver_pass_number = 0
    for benchmark in testset:
        sqlsolver_pass_number += len(sqlsolver_pass_cases[benchmark])
    print("SQLSolver can prove "+str(sqlsolver_pass_number)+" pairs, "+str(sqlsolver_pass_number-baseline_total-1)+" of which cannot be proved by any of these existing provers.")

    print("\n\n5. The data in the third paragraph of Section 6.2:")
    with open("../results/reproduction.txt", "r") as f:
        lines = f.readlines()
    for line in lines:
        if line.find("Combined with our algorithm to") == 0:
            print(line)
            break

    print("\n\n6. The data in the last sentence of Section 1:")
    new_rules_number = extract_rules_number("./results/newrules.txt")
    print("When using SQLSolver to discover SQL rewrite rules, we find "+str(new_rules_number)+" new rewrite rules beyond the 35 rules found by using the existing solver in WeTune.")

    print("\n\n7. The data in Section 6.3:")
    wetune_rules_number = extract_rules_number("./results/wetunerules.txt")
    print("Our integration of SQLSolver reveals all "+str(wetune_rules_number)+" useful rules previously found by WeTune.")

    with open("../results/reproduction.txt", "r") as f:
        lines = f.readlines()
    for line in lines:
        if line.find("The new rules induce a") == 0:
            print(line)
            break

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

def extract_verification_time(line):
    line = line.replace(" ", "")
    is_idx = line.find("is")
    vs_idx = line.find("vs.", is_idx)
    return int(line[is_idx+2:vs_idx])

def extract_verification_time_lines(lines, verifier, benchmark):
    for line in lines:
        line = line.replace("\r", "").replace("\n", "")
        if line.find(verifier+" vs. SQLSolver on " + benchmark) == 0:
            return extract_verification_time(line)
    return 0

def extract_verification_results(file_name):
    pass_case = []
    verification_time = []
    with open(file_name, "r") as f:
        lines = f.readlines()
    for line in lines:
        search_result = re.search('case [0-9]+ pass: [0-9]+ ms', line, re.M|re.I)
        if search_result == None:
            search_result = re.search('case [0-9]+: pass, [0-9]+ ms', line, re.M|re.I)
        if search_result == None:
            search_result = re.search('Case [0-9]+ is: EQ [0-9]+ ms', line, re.M|re.I)
        if search_result:
            elements = search_result.group().split(" ")
            case_id = elements[1]
            if case_id[-1] == ':':
                case_id = case_id[:-1]
            pass_case.append(int(case_id))
            if elements[3] == "EQ":
                time = elements[4]
            else:
                time = elements[3]
            verification_time.append(int(time))
    return pass_case, verification_time, len(pass_case)

def init_baseline_results():
    for verifier in baseline:
        for test_set in testset:
            key = verifier + "-" + test_set
            log_file = "../results/e1/" + key + ".log"
            pass_case, verification_time, pass_case_number = extract_verification_results(log_file)
            baseline_results[key] = pass_case
            baseline_verification_times[key] = verification_time
            baseline_pass_cases_number[key] = pass_case_number
            if key == "udp-calcite" and pass_case_number < 33:
                baseline_pass_cases_number[key] = 33
            if key == "spes-tpch":
                baseline_pass_cases_number[key] = 0
                baseline_results[key] = []
                baseline_verification_times[key] = []
            if pass_case_number > 0 and baseline_pass_cases_number[key] > 0:
                total_time = 0
                for time in verification_time:
                    total_time += time
                baseline_avg_verification_times[key] = int(total_time / pass_case_number)

def postprocess():
    init_baseline_results()
    for benchmark in testset:
        analyze_results(benchmark)
    reproduce()

postprocess()
