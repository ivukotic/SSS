export HOME=/home/ivukotic

export SSS_UNIVERSE=vanilla
export SSS_REQUIREMENTS='(UidDomain == "osg-gk.mwt2.org") &&  ( regexp("^uc3.*", TARGET.Machine,"IM") == True )'

export STORAGEPREFIX=root://fax.mwt2.org/

cd ~/SSS/SSSexecutor

if [ -f "/tmp/.SSSexecutor.proc" ]
then
	echo "found /tmp/.SSSexecutor.proc. reading proc number"
	proc=$(head -1 /tmp/.SSSexecutor.proc)
	echo "checking if proces $proc exists"
	ps -p $proc | grep java
	if [ $? -eq 0 ]
	then
		echo "process exists. doing nothing"
	else
		echo "process does not exist. restarting."
		java -Dorg.slf4j.simpleLogger.defaultLogLevel=info -jar target/SSSexecutor-1.0-jar-with-dependencies.jar &
        	echo "process restarted"
	fi
else
	echo "file /tmp/.SSSexecutor.proc not found. starting SSSexecutor. "
        java -Dorg.slf4j.simpleLogger.defaultLogLevel=info -jar target/SSSexecutor-1.0-jar-with-dependencies.jar &
        echo "process started"
fi
