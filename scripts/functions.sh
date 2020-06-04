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
NUM_PARTITIONS=""
CHUNKS=10
MAX_JOBS=8
PYTHON="python3"
DISK_SPACE_CHECK_DIR=""

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
    --check-diskspace-of-directory=*)
    DISK_SPACE_CHECK_DIR="${i#*=}"
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

if [[ $SCHEMA == *"part"* ]]; then
  if [ -z $NUM_PARTITIONS ]; then
    NUM_PARTITIONS=32
    echo "Partitioned schema selected but no number of partitions given. Defaulting to: ${NUM_PARTITIONS} partitions"
  fi
fi

function check_and_set_python {
    python_minor=$(python3 -c 'import sys; print(sys.version_info[1])')
    bin_path="/usr/bin"

    if [ $python_minor -lt 6 ]; then
        echo "${bin_path}/python3 points to a non-supported version. Python >= 3.6 is required."
        echo "Will try to use python3.6 directly."
        if [ -f ${bin_path}/python3.6 ]; then
            echo "Found ${bin_path}/python3.6"
            PYTHON="python3.6"
        else
            echo "ERROR: Could not find ${bin_path}/python3.6"
            exit 1
        fi
    fi
}

function check_program_and_fail {
    PROGRAM=$1
    HINT=$2
    if ! hash $PROGRAM 2> /dev/null; then
        echo "ERROR: Could not find $PROGRAM. $HINT"
        exit -1
    fi
}

check_program_and_fail "jinja2" "Did you run 'pip3 install -r requirements.txt'? Is PATH setup properly?"
check_program_and_fail "psql" "Is it installed? Is PATH setup properly?"