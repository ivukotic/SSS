SSS
===

SlimSkimService for ATLAS D3DP files.
It has two parts: 
1. a simple web server: SSSserver 
Only one of these should be running at any time. I serves as a backend to SSS web page:
http://ivukotic.web.cern.ch/ivukotic/SSS/index.asp
It receives requests, runs dq2 commands and simple root codes that extracts info needed to support web page.
Finally it sends info to an Oracle DB.

2. SSSexecutor
should be running at all times at one or more Condor queues. It takes SSS tasks from the DB and submits them.
Finally it should (not yet implemented) dq2-put files in the DS. 
While now it uses a standard filter-and-merge script, a new version will from time to time communicate with OracleDB it's status, machine, cpu eff., outputsizes, output events etc.
