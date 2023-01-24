In order to run the processes, scripts can be found in the /scripts folder. These are all shell scripts, but the commands can be copied and pasted to and system with java installed.

Expected formats:

Controller.sh
> $1: Port for the controller to listen on (local)
> $2: Replication factor for stored files
> $3: Maximum request timeout (ms) before a store process is considered dead
> $4: The period between store process rebalances (s)


Dstore.sh
> $1: Port for the store to listen on (local)
> $2: Port that the controller is listening on (local)
> $3: Maximum request timeout (ms) before a the controller process is considered dead
> $4: The relative path to folder in which the store will use (this will be emptied on startup)

Client.sh
> no arguments
> currently runs automated storage of 3 files on startup 
> only functional where the controller is hosted on port 12345, and 3 dstores are hosted on ports 1234, 1235 and 1236
> after setup, input uses: "*port* *function* *message*"