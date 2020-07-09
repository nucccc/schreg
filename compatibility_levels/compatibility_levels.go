package compatibility_levels

/*
A packege intended to contain strings used to represent predefined compatibility levels of Confluent
Schema Registry's compatibility levels
*/

type CompatibilityLevel string

const (
	BackwardCL           CompatibilityLevel = "BACKWARD"
	BackwardTransitiveCL CompatibilityLevel = "BACKWARD_TRANSITIVE"
	ForwardCL            CompatibilityLevel = "FORWARD"
	ForwardTransitiveCL  CompatibilityLevel = "FORWARD_TRANSITIVE"
	FullCL               CompatibilityLevel = "FULL"
	FullTransitiveCL     CompatibilityLevel = "FULL_TRANSITIVE"
	NoneCL               CompatibilityLevel = "NONE"
	InvalidCL            CompatibilityLevel = ""
)
