package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-country-exconf-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-country-exconf-rmq-kube/DPFM_API_Output_Formatter"
	"data-platform-api-country-exconf-rmq-kube/database"
	"sync"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

type ExistenceConf struct {
	ctx context.Context
	db  *database.Mysql
	l   *logger.Logger
}

func NewExistenceConf(ctx context.Context, db *database.Mysql, l *logger.Logger) *ExistenceConf {
	return &ExistenceConf{
		ctx: ctx,
		db:  db,
		l:   l,
	}
}

func (e *ExistenceConf) Conf(input *dpfm_api_input_reader.SDC) *dpfm_api_output_formatter.Country {
	country := *input.Country.Country
	notKeyExistence := make([]string, 0, 1)
	KeyExistence := make([]string, 0, 1)

	existData := &dpfm_api_output_formatter.Country{
		Country:       country,
		ExistenceConf: false,
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if !e.confCountry(country) {
			notKeyExistence = append(notKeyExistence, country)
			return
		}
		KeyExistence = append(KeyExistence, country)
	}()

	wg.Wait()

	if len(KeyExistence) == 0 {
		return existData
	}
	if len(notKeyExistence) > 0 {
		return existData
	}

	existData.ExistenceConf = true
	return existData
}

func (e *ExistenceConf) confCountry(val string) bool {
	rows, err := e.db.Query(
		`SELECT Country 
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_country_country_data 
		WHERE Country = ?;`, val,
	)
	if err != nil {
		e.l.Error(err)
		return false
	}

	for rows.Next() {
		var country string
		err := rows.Scan(&country)
		if err != nil {
			e.l.Error(err)
			continue
		}
		if country == val {
			return true
		}
	}
	return false
}
