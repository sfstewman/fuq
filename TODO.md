Quick to-do list of features for fuq:

1. [ ] Fix restarting the fuqsrv foreman so it correctly retains state from
       the database

2. [X] Write a tool to examine/dump the offline job database

3. [ ] Add the ability to queue (limited) commands:

	* Job commands (what we currently have)
	* Report commands (dump status to file, etc.)
	* Stop queue: stops the queue when the commands/jobs in front of
	  it are finished

4. [ ] Split the database store into several stores to help minimize contention:

	* Cookie jar
	* Job store
	* Task store

5. [ ] Change the nature of connections between the workers and the foreman:

	* Currently each individual worker connects to the foreman and
	  requests a task
	* Change this so each worker process keeps a persistent
	  connection and updates the foreman when the number of
	  available workers changes.

