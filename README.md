# GoAPIcall

Description:
topos - PostgreSQL database dump from local.
main.go - Go program to call the Building footprint API, fetch the data and load them to the relation schema defined in the database.
createAPI.go - Go program to create RESTful API endpoints in localhost to fetch relevant queries from the database.

Import topos database file to your local postgresql database.

Run main.go: Before running main.go install package 
              go get github.com/lib/pq
             Run using command : go run main.go
             
Run createAPI.go: Before running main.go install package 
                    go get github.com/lib/pq
                    go get github.com/gorilla/mux
                  Run using command : go run createAPI.go
