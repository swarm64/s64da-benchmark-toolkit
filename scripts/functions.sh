intexit() {
    kill -HUP -$$
}

hupexit () {
    echo
    echo "Interrupted"
    exit
}

trap hupexit HUP
trap intexit INT

set -e
DB_HOST="localhost"
DB_PORT=5432
NUM_PARTITIONS=32
CHUNKS=10
MAX_JOBS=8

function print_help {
echo "
Usage instructions:
    $0 \\
      --dbname=<db to use> \\
      --schema=<schema-to-use> \\
      --scale-factor=<scale-factor-to-use>

    Optional arguments:
      --db-host         The host of the database to connect to.
                        Default: localhost

      --db-port         The port of the database to connec to.
                        Default: 5432

      --num-partitions  How many partitions to use (if applicable)
                        Default: 32

      --chunks          How many parallel data chunks to generate
                        Default: 10

      --data-dir        The path to the directory holding data to
                        be ingested (if applicable)

      --max-jobs        Limits the overall amount of parallelism.
                        Default: 8"

}

for i in "$@"
do
case $i in
    --num-partitions=*)
    NUM_PARTITIONS="${i#*=}"
    shift
    ;;
    --schema=*)
    SCHEMA="${i#*=}"
    shift
    ;;
    --scale-factor=*)
    SCALE_FACTOR="${i#*=}"
    shift
    ;;
    --chunks=*)
    CHUNKS="${i#*=}"
    shift
    ;;
    --dbname=*)
    DB="${i#*=}"
    if [[ $DB =~ [A-Z] ]]; then
        echo "ERROR: Uppercase letters are not allowed in database names." && exit -1
    fi
    shift
    ;;
    --db-host=*)
    DB_HOST="${i#*=}"
    shift
    ;;
    --db-port=*)
    DB_PORT="${i#*=}"
    shift
    ;;
    --data-dir=*)
    DATA_DIR="${i#*=}"
    shift
    ;;
    --max-jobs=*)
    MAX_JOBS="${i#*=}"
    shift
    ;;
    *)
        echo "Unknown option $i"
        print_help
        exit -1
    ;;
esac
done

if [ "$DB_HOST" = "socket" ]; then
    PSQL="psql -U postgres"
else
    PSQL="psql -U postgres -h ${DB_HOST} -p ${DB_PORT}"
fi

function check_and_fail {
    if [ -z ${!1+x} ]; then
        echo "ERROR: ${2} is not set."
        print_help
        exit -1
    fi
}

check_and_fail DB "--dbname"
check_and_fail SCHEMA "--schema"
check_and_fail SCALE_FACTOR "--scale-factor"

function check_program_and_fail {
    PROGRAM=$1
    HINT=$2
    if ! hash $PROGRAM 2> /dev/null; then
        echo "ERROR:    $PROGRAM    is not installed. $HINT"
        exit -1
    fi
}

check_program_and_fail "jinja2" "Did you run 'pip3 install -r requirements.txt'?"
check_program_and_fail "psql" "Is it installed? Is PATH setup properly?"

function wait_for_pg {
    set +e

    PSQL_UP=0
    for i in {0..120}; do
        $PSQL -c "SELECT 1"
        if [ $? -eq 0 ]; then
            PSQL_UP=1
            break
        fi
        sleep 1
    done

    if [ $PSQL_UP -ne 1 ]; then
        echo "PSQL did not come up."
        exit -1
    fi

    set -e
}

function psql_exec_file {
    $PSQL -d $DB -f "$1"
}

function psql_exec_cmd {
    $PSQL -d $DB -c "$1"
}

function prepare_db {
    ENCODING=$1
    $PSQL -c "DROP DATABASE IF EXISTS $DB"
    $PSQL -c "CREATE DATABASE $DB WITH ENCODING ${ENCODING} TEMPLATE TEMPLATE0"
}

function run_if_exists {
    FILE=$SCHEMA/$1
    if [ -f "$FILE" ]; then
        echo "Executing $FILE"
        psql_exec_file $FILE
    fi
}

function deploy_schema {
    SCHEMA=$1
    NUM_PARTITIONS=$2
    SCHEMA_FILE=`mktemp`
    jinja2 $SCHEMA/schema.sql -D num_partitions=$NUM_PARTITIONS > $SCHEMA_FILE
    psql_exec_file $SCHEMA_FILE
}
