#!/usr/bin/env bash

GIT=$(which git)
[ $? -eq 1 ] && exit 1

nodeID=0
heapSize=$1
blockSize=$((2**22)) 

rounds=1
nOperations=$2
nThreads=$3
initChunks=100
initMin=16
initMax=48

minDelay=0
maxDelay=0
minSize=16
maxSize=2048

get_branch_names() {
    branches=$($GIT branch)
    printf "%s\n" "$branches"|sed -e "s#\*##g"
}

for branch in $(get_branch_names)
do
    $GIT checkout $branch &> /dev/null

    for readProb in $(LANG=en seq 0.0 0.25 1.0); do
        for createProb in $(LANG=en seq 0.0 0.25 1.0); do
            for changeProb in $(LANG=en seq 0.0 0.25 1.0); do
                $PWD/start-dxram-memory.sh $nodeID $heapSize $blockSize $branch\
                    $rounds $nOperations $nThreads $initChunks $initMin $initMax \
                    $createProb $readProb $changeProb $minDelay $maxDelay \
                    $minSize $maxSize
            done
        done
    done
done




