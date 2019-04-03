package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"io/ioutil"
	"net/http"
)

// Type interface containing field structures to fetch from the API
type Footprint struct {
	Base_bbl string
	Name string
	Bin string
	Cnstrct_yr string
	Geomsource string
	Groundelev string
	Heightroof string
	Lststatype string
	The_geom struct{
		Type string
		Coordinates []float32
	}
}

// Fetches data from the API and stores it to the type structure
func getBuildingData(data []byte) ([]Footprint, error)  {
	footprint := make([]Footprint, 0)
	err := json.Unmarshal(data, &footprint)
	if err != nil {
		fmt.Printf("Data Unmarshall error %s\n", err)
	}
	return footprint, err
}

// Calls the API, reads all the data and then converts it to the type structure
func footprintAPIcall(api string) []Footprint {
	responseBuilding, err := http.Get(api)
	if err != nil {
		fmt.Printf("API Call failed %s\n", err)
	} else {
		data, err := ioutil.ReadAll(responseBuilding.Body)
		if err != nil {
			fmt.Printf("Data format error %s\n", err)
		} else {
			footprint, err := getBuildingData([]byte(data))
			if err == nil {
				fmt.Println("Data successfully fetched from API")
				return footprint
			}
		}
	}
	return nil
}

// Connects to the database with the provided details
// stores the type structure to the already created normalized relational tables
func postgreConnectAndInsert(footprints []Footprint) {
	const (
		host = "localhost"
		port = 5432
		user = "postgres"
		password = "Ritaban10"
		dbname = "topos"
	)

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		fmt.Printf("Database connection error %s\n", err)
	} else {
		defer db.Close()
		err = db.Ping()
		if err != nil {
			fmt.Printf("Database connection not 100% %s\n", err)
		} else {
			fmt.Println("Database connection successful")
			geomSource := make(map[string]int)
			sourceId := 1
			stateType := make(map[string]int)
			state := 1
			geomType := make(map[string]int)
			geom := 1
			for _, footprint := range footprints {
				if geomSource[footprint.Geomsource] == 0 {
					geomSource[footprint.Geomsource] = sourceId
					sqlStatement := `INSERT INTO geom_source (id, source) VALUES ($1, $2)`
					_, err := db.Exec(sqlStatement, sourceId, footprint.Geomsource)
					if err != nil {
						fmt.Printf("Error %s\n", err)
					}
					sourceId += 1
				}
				if stateType[footprint.Lststatype] == 0 {
					stateType[footprint.Lststatype] = state
					sqlStatement := `INSERT INTO state_type (id, type) VALUES ($1, $2)`
					_, err := db.Exec(sqlStatement, state, footprint.Lststatype)
					if err != nil {
						fmt.Printf("Error %s\n", err)
					}
					state += 1
				}
				if geomType[footprint.The_geom.Type] == 0 {
					geomType[footprint.The_geom.Type] = geom
					sqlStatement := `INSERT INTO geom_type (id, type) VALUES ($1, $2)`
					_, err := db.Exec(sqlStatement, geom, footprint.The_geom.Type)
					if err != nil {
						fmt.Printf("Error %s\n", err)
					}
					geom += 1
				}

				sqlStatementBuilding := `INSERT INTO buildings (id, name, construct_year, geom_source,
								last_state_type) VALUES ($1, $2, $3, $4, $5)`
				_, errBuilding := db.Exec(sqlStatementBuilding, footprint.Base_bbl, footprint.Name, footprint.Cnstrct_yr,
					geomSource[footprint.Geomsource], stateType[footprint.Lststatype])
				if errBuilding != nil {
					fmt.Printf("Error building %s for base bbl %s\n", err, footprint.Base_bbl)
				}

				sqlStatementDimension := `INSERT INTO building_dimensions (building, ground_elevation, roof_height) 
								VALUES ($1, $2, $3)`
				_, errDimension := db.Exec(sqlStatementDimension, footprint.Base_bbl, footprint.Groundelev,
					footprint.Heightroof)
				if errDimension != nil {
					fmt.Printf("Error building_dimension %s for base bbl %s\n", errDimension, footprint.Base_bbl)
				}

				sqlStatementGeom := `INSERT INTO building_geom (building, geom, coordinate_x, coordinate_y) 
								VALUES ($1, $2, $3, $4)`
				_, errGeom := db.Exec(sqlStatementGeom, footprint.Base_bbl, geomType[footprint.The_geom.Type],
					footprint.The_geom.Coordinates[0], footprint.The_geom.Coordinates[1])
				if errGeom != nil {
					fmt.Printf("Error building_geom %s for base bbl %s\n", errGeom, footprint.Base_bbl)
				}
			}
		}
	}
}

// Main function executing the program
func main() {
	footprint := footprintAPIcall("https://data.cityofnewyork.us/resource/9ey5-eyh6.json")
	if footprint != nil {
		postgreConnectAndInsert(footprint)
	}
}

