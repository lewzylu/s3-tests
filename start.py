# -*- coding=utf-8
import os
import time
import nuke

region = os.environ['COS_REGION']
test_num = 0
succ_num = 0
fail_num = 0
date = time.strftime("%Y-%m-%d", time.localtime())
try:
    os.makedirs("./result/" + region)
except:
    pass
try:
    os.makedirs("./debug/" + region)
except:
    pass
try:
    os.makedirs("./daily/" + region)
except:
    pass
fail_case = []
with open("test_cases.txt", "r") as f:
    with open("./result/" + region + "/" + date, "w") as fresult:
        for line in f:
            line = line.strip("\r\n")
            command = "S3TEST_CONF=cos.conf ./virtualenv/bin/nosetests {testCase} -s --debug=boto --all-modules -v > ./debug/{region}/{testCase}.txt 2>&1".format(testCase=line, region=region)
            rt = os.system(command)
            if 0 == rt:
                print line + " success"
                fresult.write(line + " success" + "\r\n")
                succ_num += 1
            else:
                print line + " failure"
                fresult.write(line + " failure" + "\r\n")
                fail_case.append(line)
                fail_num += 1
        test_num += 1
nuke.nuke()
with open("./daily/" + region + "/" + date, "w") as f:
    f.write(region + "\r\n")
    f.write(str(test_num) + "," + str(succ_num) + "," + str(fail_num) + "\r\n")
    for i in fail_case:
        f.write(i + "\r\n")
    
