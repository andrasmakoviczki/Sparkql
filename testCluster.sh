#!/bin/bash
Red='\033[0;31m'
Green='\033[0;32m'
NC='\033[0m' # No Color

testeld(){
	inputVertex=$1
	inputEdge=$2
	query=$3
	expected=$4
	echo "TEST"
	echo "Query: $query" 
	spark-submit --class Main --driver-memory 4g --executor-memory 4g  --deploy-mode client --master spark://sparkql-ubuntu:7077 --num-executors 1 /home/sparkuser/Sparkql/target/scala-2.11/sparkql_2.11-1.0.jar "$inputVertex" "" "$query" 1>result.log 2>error.log

  #wait until process done
	pid=$(echo $!)
  while kill -0 $pid 2> /dev/null; do sleep 1; done;
  
	count=$(grep "RESULT2" result.log | cut -d" " -f2)
	if [[ -n "$count" && "$count" -eq "$expected" ]] 
	then
		printf "[${Green}OK${NC}]\n"
	else 
		printf "[${Red}NOK${NC}]: got: $count instead of $expected\n"
	fi
}

inputVertex="/home/sparkuser/proba/testelo/LUBM_1000.n3"
inputEdge=$inputVertex

#LUBM 1
echo "LUBM 1"

query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#GraduateStudent> . ?X <http://spark.elte.hu#takesCourse> <http://www.Department0.University1000.edu/GraduateCourse0>"
expected=3
testeld "$inputVertex" "$inputEdge" "$query" "$expected"

#LUBM 2
echo "LUBM 2"

query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#GraduateStudent> . ?Y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#University> . ?Z <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Department> . ?X <http://spark.elte.hu#memberOf> ?Z . ?Z <http://spark.elte.hu#subOrganizationOf> ?Y . ?X <http://spark.elte.hu#undergraduateDegreeFrom> ?Y "
expected=4
testeld "$inputVertex" "$inputEdge" "$query" "$expected"

#LUBM 3
echo "LUBM 3"

query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Publication> . ?X <http://spark.elte.hu#publicationAuthor> <http://www.Department0.University1000.edu/AssistantProfessor0>"
expected=8
testeld "$inputVertex" "$inputEdge" "$query" "$expected"

#LUBM 4
echo "LUBM 4"

query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Professor> . ?X <http://spark.elte.hu#worksFor> <http://www.Department0.University1000.edu> . ?X <http://spark.elte.hu#name> ?Y1 . ?X <http://spark.elte.hu#emailAddress> ?Y2 . ?X <http://spark.elte.hu#telephone> ?Y3"
expected=27
testeld "$inputVertex" "$inputEdge" "$query" "$expected"

#LUBM 5
echo "LUBM 5"

query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Person> . ?X <http://spark.elte.hu#memberOf> <http://www.Department0.University1000.edu>"
expected=373
testeld "$inputVertex" "$inputEdge" "$query" "$expected"

#LUBM 6
echo "LUBM 6"

query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Student>"
expected=23734
testeld "$inputVertex" "$inputEdge" "$query" "$expected"

#LUBM 7
echo "LUBM 7"

query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Student> . ?Y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Course> . ?X <http://spark.elte.hu#takesCourse> ?Y . <http://www.Department0.University1000.edu/AssociateProfessor0> <http://spark.elte.hu#teacherOf> ?Y"
expected=52
testeld "$inputVertex" "$inputEdge" "$query" "$expected"

#LUBM 8
echo "LUBM 8"

query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Student> . ?Y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Department> . ?X <http://spark.elte.hu#memberOf> ?Y . ?Y <http://spark.elte.hu#subOrganizationOf> <http://www.University1000.edu> . ?X <http://spark.elte.hu#emailAddress> ?Z"
expected=12725
testeld "$inputVertex" "$inputEdge" "$query" "$expected"


#LUBM 9
echo "LUBM 9"

query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Student> . ?Y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Faculty> . ?Z <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Course> . ?X <http://spark.elte.hu#advisor> ?Y . ?Y <http://spark.elte.hu#teacherOf> ?Z . ?X <http://spark.elte.hu#takesCourse> ?Z"
expected=310
testeld "$inputVertex" "$inputEdge" "$query" "$expected"


#LUBM 10
echo "LUBM 10"

query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Student> . ?X <http://spark.elte.hu#takesCourse> <http://www.Department0.University1000.edu/GraduateCourse0>"
expected=3
testeld "$inputVertex" "$inputEdge" "$query" "$expected"
