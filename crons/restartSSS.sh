export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase
export HOME=/home/ivukotic

source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh
source ${ATLAS_LOCAL_ROOT_BASE}/packageSetups/atlasLocalDQ2ClientSetup.sh --skipConfirm --dq2ClientVersion current

voms-proxy-init -cert $HOME/.globus/usercert.pem -key $HOME/.globus/userkey.pem -voms atlas -pwstdin < $HOME/gridlozinka.txt

if [ -f "/tmp/.SSSserver.proc" ] 
then
	echo "found /tmp/.SSSserver.proc. reading proc number"
	proc=$(head -1 /tmp/.SSSserver.proc)
	echo "checking if proces $proc exists"
	ps -p $proc | grep java
	if [ $? -eq 0 ]
	then
		echo "process exists. doing nothing." 
        exit 0
	else
		echo "process does not exist. removing proc file."
        rm /tmp/.SSSserver.proc
	fi
fi    

echo "file /tmp/.SSSserver.proc not found. starting SSSserver. "
source ${ATLAS_LOCAL_ROOT_BASE}/packageSetups/atlasLocalROOTSetup.sh --rootVersion current
cd ~/SSS/SSSserver
export STORAGEPREFIX=root://glrd.usatlas.org/
make
java -Dorg.slf4j.simpleLogger.defaultLogLevel=info -jar target/SSSserver-1.0-jar-with-dependencies.jar &
echo "process started"

