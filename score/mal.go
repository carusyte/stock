package score

//Mal the scorer
type Mal struct{

}

//GetFieldStr returns the string representation of the specified field.
func (*Mal) GetFieldStr(name string) string {
	panic("implement me")
}

//Get result for specified stocks
func (*Mal) Get(stock []string, limit int, ranked bool) (r *Result) {
	panic("implement me")
}

//Geta gets result for all stocks
func (*Mal) Geta() (r *Result) {
	panic("implement me")
}

//ID for the scorer
func (*Mal) ID() string {
	panic("implement me")
}

//Fields for the scorer
func (*Mal) Fields() []string {
	panic("implement me")
}

//Description for the scorer
func (*Mal) Description() string {
	panic("implement me")
}

