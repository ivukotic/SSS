#!/bin/zsh

export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase
alias setupATLAS='source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh'
export ALRB_localConfigDir=$HOME/localConfig
setupATLAS
alias glite='localSetupGLite'
alias dq2='localSetupDQ2Client'
asetup 17.6.0,noTest
filter-and-merge-d3pd.py --help > test.out
