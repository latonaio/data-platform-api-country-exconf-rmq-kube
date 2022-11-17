package dpfm_api_input_reader

import (
	"data-platform-api-country-exconf-rmq-kube/DPFM_API_Caller/requests"
)

func (sdc *SDC) ConvertToCountry() *requests.Country {
	data := sdc.Country
	return &requests.Country{
		Country: data.Country,
	}
}
