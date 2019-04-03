package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
)

var db *sql.DB

// Building type structure containing id, name and construct year.
type Building struct {
	Id int64 `json:"id"`
	Name string `json:"name"`
	ContructYear int `json:"constructYear"`
}

// Building Dimension type structure
// containing id, name, construct year, ground elevation and roof height
type BuildingDimension struct {
	Id int64 `json:"id"`
	Name string `json:"name"`
	ContructYear int `json:"constructYear"`
	GroundElevation float32 `json:"groundElevation"`
	RoofHeight float32 `json:"roofHeight"`
}

// Building Source type structure
// containing source and total count of buildings for that source
type SourceCount struct {
	Source string `json:"source"`
	Buildings int64 `json:"buildings"`
}

// Building Geom type structure
// containing source and total count of buildings for that source
type TypeCount struct {
	Type string `json:"geomType"`
	Buildings int64 `json:"buildings"`
}

// Building Details for Coordinates type structure
// containing building details within the coordinate range
type BuildingCoordinate struct {
	Id int64 `json:"id"`
	Name string `json:"name"`
	ContructYear int `json:"constructYear"`
	GroundElevation float32 `json:"groundElevation"`
	RoofHeight float32 `json:"roofHeight"`
	GeomType string `json:"geomType"`
	CoordinateX float32 `json:"xCoordinate"`
	CoordinateY float32 `json:"yCoordinate"`
}

type Buildings []Building

type BuildingDimensions []BuildingDimension

type SourceCounts []SourceCount

type TypeCounts []TypeCount

type BuildingsCoordinate []BuildingCoordinate

// API GET call fetching all buildings
func allBuildings(w http.ResponseWriter, r *http.Request)  {
	const (
		host = "localhost"
		port = 5432
		user = "postgres"
		password = "Ritaban10"
		dbname = "topos"
	)

	buildings := Buildings{}

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
			rows, err := db.Query(`SELECT id, name, construct_year FROM buildings`)
			if err != nil {
				fmt.Printf("Error in query %s\n", err)
			}
			defer rows.Close()
			for rows.Next() {
				building := Building{}
				err := rows.Scan(&building.Id, &building.Name, &building.ContructYear,)
				if err != nil {
					panic(err.Error())
				} else {
					buildings = append(buildings, building)
				}
			}

			fmt.Fprintf(w, "All Buildings")
			json.NewEncoder(w).Encode(buildings)
		}
	}
}


// API GET call fetching all buildings constructed in the input year
func yearWiseBuildings(w http.ResponseWriter, r *http.Request)  {
	const (
		host = "localhost"
		port = 5432
		user = "postgres"
		password = "Ritaban10"
		dbname = "topos"
	)

	buildings := Buildings{}

	vars := mux.Vars(r)
	year := vars["year"]
	intYear, err := strconv.Atoi(strings.Trim(year, " "))
	if err != nil {
		fmt.Fprintf(w, "Incorrect Year format (YYYY required)")
	} else {
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
				rows, err := db.Query(`select id, name, construct_year from buildings
					where construct_year = $1`, intYear)
				if err != nil {
					fmt.Printf("Error in query %s\n", err)
				}
				defer rows.Close()
				for rows.Next() {
					building := Building{}
					err := rows.Scan(&building.Id, &building.Name, &building.ContructYear,)
					if err != nil {
						panic(err.Error())
					} else {
						buildings = append(buildings, building)
					}
				}

				fmt.Fprintf(w, "All Buildings for %s", year)
				json.NewEncoder(w).Encode(buildings)
			}
		}
	}
}

// API GET call fetching the dimensions of the input building
func allDetail(w http.ResponseWriter, r * http.Request) {
	const (
		host = "localhost"
		port = 5432
		user = "postgres"
		password = "Ritaban10"
		dbname = "topos"
	)

	building := BuildingDimension{}

	vars := mux.Vars(r)
	args := vars["building"]
	id, err := strconv.Atoi(args)
	if err != nil {
		fmt.Fprintf(w, "Incorrect Year format (YYYY required)")
	} else {
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
				err := db.QueryRow(`select b.id, b.name, b.construct_year,
					bd.ground_elevation, bd.roof_height
						from buildings as b
						join building_dimensions as bd on b.id = bd.building
						where b.id =  $1`, id).Scan(&building.Id, &building.Name,
					&building.ContructYear, &building.GroundElevation, &building.RoofHeight)
				if err != nil {
					fmt.Printf("Error in query %s\n", err)
				}

				fmt.Fprintf(w, "Building Dimensions for %s", args)
				json.NewEncoder(w).Encode(building)
			}
		}
	}
}

// API GET call fetching all buildings
// whose ground elevation is greater than the average elevation
func avgElevation(w http.ResponseWriter, r * http.Request) {
	const (
		host = "localhost"
		port = 5432
		user = "postgres"
		password = "Ritaban10"
		dbname = "topos"
	)

	buildings := BuildingDimensions{}

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
			rows, err := db.Query(`select b.id, b.name, b.construct_year,
					bd.ground_elevation, bd.roof_height
					from buildings as b
					join building_dimensions as bd on b.id = bd.building
					where bd.ground_elevation > (
						select avg(ground_elevation) from building_dimensions
					)`)
			if err != nil {
				fmt.Printf("Error in query %s\n", err)
			}
			defer rows.Close()
			for rows.Next() {
				building := BuildingDimension{}
				err := rows.Scan(&building.Id, &building.Name,
					&building.ContructYear, &building.GroundElevation, &building.RoofHeight)
				if err != nil {
					panic(err.Error())
				} else {
					buildings = append(buildings, building)
				}
			}

			fmt.Fprintf(w, "Buildings and their dimensions where ground elevation" +
				" greater than the average elevation\n")
			json.NewEncoder(w).Encode(buildings)
		}
	}
}

// API GET call fetching the building count based on the source or geom type
func numDataSource(w http.ResponseWriter, r * http.Request) {
	const (
		host = "localhost"
		port = 5432
		user = "postgres"
		password = "Ritaban10"
		dbname = "topos"
	)

	sourceCounts := SourceCounts{}
	typeCounts := TypeCounts{}

	vars := mux.Vars(r)
	group := vars["group"]
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
			if group == "source" {
				rows, err := db.Query(`select gs.source, count(b.id) as buildings
					from buildings as b
					join geom_source as gs on b.geom_source = gs.id
					group by gs.id`)
				if err != nil {
					fmt.Printf("Error in query %s\n", err)
				}
				defer rows.Close()
				for rows.Next() {
					source := SourceCount{}
					err := rows.Scan(&source.Source, &source.Buildings)
					if err != nil {
						panic(err.Error())
					} else {
						sourceCounts = append(sourceCounts, source)
					}
				}

				fmt.Fprintf(w, "Number of Buildings information based on source\n")
				json.NewEncoder(w).Encode(sourceCounts)
			} else if group == "type" {
				rows, err := db.Query(`select gt.type, count(bg.building) as buildings
					from geom_type as gt 
					join building_geom as bg on gt.id = bg.geom
					group by gt.id`)
				if err != nil {
					fmt.Printf("Error in query %s\n", err)
				}
				defer rows.Close()
				for rows.Next() {
					typeC := TypeCount{}
					err := rows.Scan(&typeC.Type, &typeC.Buildings)
					if err != nil {
						panic(err.Error())
					} else {
						typeCounts = append(typeCounts, typeC)
					}
				}

				fmt.Fprintf(w, "Number of Buildings information based on type\n")
				json.NewEncoder(w).Encode(typeCounts)
			}
		}
	}
}

// API GET call fetching all building details between the coordinate range input
func buildingCoordinates(w http.ResponseWriter, r *http.Request)  {
	const (
		host = "localhost"
		port = 5432
		user = "postgres"
		password = "Ritaban10"
		dbname = "topos"
	)

	buildingsCoord := BuildingsCoordinate{}

	vars := mux.Vars(r)
	x_range := vars["x_range"]
	y_range := vars["y_range"]
	x_range_str := strings.Split(x_range, ",")
	y_range_str := strings.Split(y_range, ",")
	x_min, err := strconv.ParseFloat(strings.Trim(x_range_str[0], " "), 32)
	x_max, err := strconv.ParseFloat(strings.Trim(x_range_str[1], " "), 32)
	y_min, err := strconv.ParseFloat(strings.Trim(y_range_str[0], " "), 32)
	y_max, err := strconv.ParseFloat(strings.Trim(y_range_str[1], " "), 32)
	if err != nil {
		fmt.Fprintf(w, "Incorrect Year format (YYYY required)")
	} else {
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
				var count = 0
				err := db.QueryRow(`select count(b.id) as buildings
					from buildings as b
					join building_geom as bg on b.id = bg.building
					where bg.coordinate_x between $1 and $2
						and bg.coordinate_y between $3 and $4`,
						x_min, x_max, y_min, y_max).Scan(&count)
				if err != nil {
					fmt.Printf("Error in query %s\n", err)
				}

				rows, err := db.Query(`select b.id, b.name, b.construct_year,
					bd.ground_elevation, bd.roof_height,
					gt.type, bg.coordinate_x, bg.coordinate_y
					from buildings as b
					join building_dimensions as bd on b.id = bd.building
					join building_geom as bg on b.id = bg.building
					join geom_type as gt on bg.geom = gt.id
					where bg.coordinate_x between $1 and $2
						and bg.coordinate_y between $3 and $4`, x_min, x_max, y_min, y_max)
				if err != nil {
					fmt.Printf("Error in query %s\n", err)
				}
				defer rows.Close()
				for rows.Next() {
					building := BuildingCoordinate{}
					err := rows.Scan(&building.Id, &building.Name, &building.ContructYear,
						&building.GroundElevation, &building.RoofHeight, &building.GeomType,
						&building.CoordinateX, &building.CoordinateY)
					if err != nil {
						panic(err.Error())
					} else {
						buildingsCoord = append(buildingsCoord, building)
					}
				}

				fmt.Fprintf(w, "Total Building for the coordinates: %d\n", count)
				fmt.Fprintf(w, "Building Details \n")
				json.NewEncoder(w).Encode(buildingsCoord)
			}
		}
	}
}

// Home page API hit endpoint
func homePage(w http.ResponseWriter, r * http.Request) {
	fmt.Fprintf(w, "Homepage for Building Footprints\n")
	fmt.Fprintf(w, "Avalaible tags : \n")
	fmt.Fprintf(w, "1. /buildings : for all buildings\n")
	fmt.Fprintf(w, "2. /buildings/{year} : for all buildings contructed in input year\n")
	fmt.Fprintf(w, "3. /dimensions/{building} : for dimensions of the input building\n")
	fmt.Fprintf(w, "4. /avgElevation : for buildings with ground elevation" +
		"greater than or equal to average elevation\n")
	fmt.Fprintf(w, "5. /numDataSource/{group} : number of buildings grouped by either " +
		"geometry 'source' or 'type' depending on input\n")
	fmt.Fprintf(w, "6. /buildings/{x_range}/{y_range} : buildings between the coordinates" +
		"passed in the input. Input eg. {x_range} : -75,70 " +
		"{y_range} : -40,40 \n")
}

// API Request Handling for all the calls
func handleRequests() {
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/", homePage)
	router.HandleFunc("/buildings", allBuildings).Methods("GET")
	router.HandleFunc("/buildings/{year}", yearWiseBuildings).Methods("GET")
	router.HandleFunc("/dimensions/{building}", allDetail).Methods("GET")
	router.HandleFunc("/avgElevation", avgElevation).Methods("GET")
	router.HandleFunc("/numDataSource/{group}", numDataSource).Methods("GET")
	router.HandleFunc("/buildings/{x_range}/{y_range}", buildingCoordinates).Methods("GET")
	fmt.Println("Listening..")
	log.Fatal(http.ListenAndServe(":8081", router))
}

// Main function executing the program
func main() {
	handleRequests()
}