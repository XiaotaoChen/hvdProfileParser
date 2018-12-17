#!/bin/bash
source /Users/cxt123/Applications/anaconda3/bin/activate conda36_base
#operator_name=broadcast
operator_name=allreduce
printAll=0
python /Users/cxt123/Reposities/hvdProfileParser/hvdProfileParser.py \
    --file $1 \
    --operator-name ${operator_name} \
    --printAll ${printAll}
