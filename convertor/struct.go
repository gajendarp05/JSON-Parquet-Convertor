package convertor

// person structure hold information about a person including ID, Name, Age etc.
type personJson struct {
	ID          string        `json:"ID"`
	Name        string        `json:"name"`
	Nationality string        `json:"nationality"`
	Age         int           `json:"age"`
	Address     []addressJson `json:"address"`
}
type addressJson struct {
	Street  int    `json:"street"`
	City    string `json:"city"`
	Pincode string `json:"pincode"`
}

// schema to write data in parquet files
type personParquet struct {
	ID          string           `parquet:"name=ID, type=BYTE_ARRAY, encoding=PLAIN_DICTIONARY, convertedtype=UTF8"`
	Name        string           `parquet:"name=name, type=BYTE_ARRAY, encoding=PLAIN_DICTIONARY, convertedtype=UTF8"`
	Nationality string           `parquet:"name=nationality, type=BYTE_ARRAY, encoding=PLAIN_DICTIONARY, convertedtype=UTF8"`
	Age         int32            `parquet:"name=age, type=INT32, convertedtype=INT_8"`
	Address     []addressParquet `parquet:"name=address, type=LIST"`
}
type addressParquet struct {
	Street  int32  `parquet:"name=street type=INT32, convertedtype=INT_8"`
	City    string `parquet:"name=city, type=BYTE_ARRAY, encoding=PLAIN_DICTIONARY, convertedtype=UTF8"`
	Pincode string `parquet:"name=pincode, type=BYTE_ARRAY, encoding=PLAIN_DICTIONARY, convertedtype=UTF8"`
}

// toParquet assign values from json to parquet for conversion
func toParquet(p personJson) personParquet {
	parquet := personParquet{
		ID:          p.ID,
		Name:        p.Name,
		Nationality: p.Nationality,
		Age:         int32(p.Age),
		Address:     make([]addressParquet, len(p.Address)),
	}
	for i := 0; i < len(p.Address); i++ {
		parquet.Address[i].Street = int32(p.Address[i].Street)
		parquet.Address[i].City = p.Address[i].City
		parquet.Address[i].Pincode = p.Address[i].Pincode
	}
	return parquet
}
