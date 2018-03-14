#!/usr/bin/env bash

## Script information
# author: Florian Hucke, florian(dot)hucke(at)hhu(dot)de
# date: 02. Feb 2018
# version: 0.2
# license: GPL V3

GIT=$(which git)
[ $? -eq 1 ] && exit 1

nodeID=0
heapSize=$1
blockSize=$((2**22)) 

rounds=$4
nOperations=$2
nThreads=$3
initChunks=1000
initMin=16
initMax=48

minDelay=0
maxDelay=0
minSize=16
maxSize=2048

readLocks=(true true false)
writeLocks=(false true true)

get_branch_names() {
    branches=$(${GIT} branch)
    printf "%s\n" "$branches"|sed -e "s#\*##g"
}

run_script() {
    $PWD/start-dxram-memory.sh ${nodeID} ${heapSize} ${blockSize} $2 $3 $1\
        ${rounds} ${nOperations} ${nThreads} ${initChunks} ${initMin} ${initMax} \
        $4 $5 $6 ${minDelay} ${maxDelay} ${minSize} ${maxSize}

}
for branch in $(get_branch_names); do
    ${GIT} checkout ${branch} &> /dev/null

    for i in $(seq 0 $((${#readLocks[*]}-1))); do
        readLock=${readLocks[$i]}
        writeLock=${writeLocks[$i]}

        run_script ${branch} ${readLock} ${writeLock} 0    0    0
        run_script ${branch} ${readLock} ${writeLock} 0    0    0.1
        run_script ${branch} ${readLock} ${writeLock} 0    0    0.5
        run_script ${branch} ${readLock} ${writeLock} 0.05 0.05 0.8
        run_script ${branch} ${readLock} ${writeLock} 0.1  0.1  0.4

    done
done
