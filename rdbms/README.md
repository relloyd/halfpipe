To add new connection types:

1. add to subcommand 'config connections add <type>' and see actions.RunConnectionAdd()
1. add to functionality to config.GetConnectionDetails()
1. ensure the connection type is valid in the action register as part of adding it to func actions.RunConnectionAdd()
1. add helper types and methods to file rdbms/shared/connection-details-generic.go
1. enhance ConnectionDetails{}.String() since this redacts user passwords. 
1. DefaultConnectionVariableSuffixes{} needs a new key to support populating ConnectionDetails from environment.
1. table definition handlers in table-definition/columns.go
1. 
 
