#!/bin/bash

chmod +x copy_meteol_structure.sh

sudo docker cp meteol namenode:/

sudo docker exec -it namenode bash 
