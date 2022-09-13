package convertor

// person structure hold information about a person including ID, Name, Age etc.
type personJson struct {
	ID          string `json:"ID"`
	Name        string `json:"name"`
	Nationality string `json:"nationality"`
	Age         int    `json:"age"`
}

// schema to write data in parquet files
type personParquet struct {
	ID          string `parquet:"name=ID, type=BYTE_ARRAY, encoding=PLAIN_DICTIONARY, convertedtype=UTF8"`
	Name        string `parquet:"name=name, type=BYTE_ARRAY, encoding=PLAIN_DICTIONARY, convertedtype=UTF8"`
	Nationality string `parquet:"name=nationality, type=BYTE_ARRAY, encoding=PLAIN_DICTIONARY, convertedtype=UTF8"`
	Age         int32  `parquet:"name=age, type=INT32, convertedtype=INT_8"`
}

// toParquet assign values from json to parquet for conversion
func toParquet(p personJson) personParquet {
	parquet := personParquet{
		ID:          p.ID,
		Name:        p.Name,
		Nationality: p.Nationality,
		Age:         int32(p.Age),
	}
	return parquet
}
