if [ -f "/tmp/.SSSexecutor.proc" ]
then
	echo "found /tmp/.SSSexecutor.proc. reading proc number"
	proc=$(head -1 /tmp/.SSSexecutor.proc)
	echo "checking if proces $proc exists"
	ps -p $proc | grep java
	if [ $? -eq 0 ]
	then
		echo "process exists. doing nothing"
        exit 0
	else
		echo "process does not exist. removing proc file."
        rm /tmp/.SSSexecutor.proc
	fi
fi

export HOME=/home/ivukotic
export dbusername=...
export dbpass=...

export SSS_UNIVERSE=vanilla
export SSS_REQUIREMENTS=(HAS_CVMFS =?= True)
export SSS_EXTRA_CONDOR_SETTINGS='+AccountingGroup="group_uct3.ivukotic"'

export STORAGEPREFIX=root://fax.mwt2.org/

cd ~/SSS/SSSexecutor

echo "file /tmp/.SSSexecutor.proc not found. starting SSSexecutor. "
java -Dorg.slf4j.simpleLogger.defaultLogLevel=info -jar target/SSSexecutor-1.0-jar-with-dependencies.jar &
echo "process started"
