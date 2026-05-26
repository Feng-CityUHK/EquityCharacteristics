#!/bin/bash
#$ -cwd
#$ -m abe
#$ -M xinhe9715@126.com
#R CMD BATCH my_program.r my_program.Output
#python3 PyProgram.py &> PyProgram.out
sas check_crsp.sas

## if you need to add cpu and memory
##$ -pe onenode 8
##$ -l m_mem_free=6G
