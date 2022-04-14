module github.com/IBM/nzgo/v12

go 1.15
require(
        github.com/IBM/nzgo v11.1.0+incompatible
)

// Do not use these versions
retract (
	v12.0.4
	v12.0.2
	v12.0.0
)
