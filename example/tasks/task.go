package exampletasks

// Task list
var (
	TaskAddition       = "add_num"
	TaskSubtraction    = "sub_num"
	TaskMultiplication = "mul_num"
	TaskErrorGenerator = "error_generator"
)

func TaskList() []string {
	return []string{
		TaskAddition,
		TaskSubtraction,
		TaskMultiplication,
		TaskErrorGenerator,
	}
}

type TaskBody struct {
	Num1 int
	Num2 int
}
