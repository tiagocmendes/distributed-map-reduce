#!/bin/bash

function usage(){
    echo "Usage: multiple_workers [-w workers_number]"
	echo "Example: multiple_workers.sh -w 4"
	exit 0
}

if [ $# -lt 1 ]; then
	usage
fi

function verify_args(){
    if [[ $1 = -* ]]; then  
        echo "ERROR: option -$opt requires an argument, not an option."
        ((OPTIND--))
        usage
    fi
}

function verify_args_2(){
    if ( $1 ); then
        echo "ERROR: option -$opt already used"
        usage
    fi
}

opt_w=false
opt_p=false
opt_w_arg=false
opt_p_arg=false

while getopts ":w:p:" opt;
do
    case $opt in
        w)
            verify_args_2 $opt_w
            opt_w_arg=$OPTARG
            verify_args "$opt_w_arg"
            opt_w=true
            ;;
        p)
            verify_args_2 $opt_p
            opt_p_arg=$OPTARG
            verify_args "$opt_p_arg"
            opt_p=true
            ;;
        :)
            echo "ERROR: option -$OPTARG requires an argument"
            usage
            ;;
        ?)  
            echo "ERROR: option unknown"
            usage
            ;;
    esac
done
shift $(($OPTIND - 1))

port=8765
if ( $opt_p ); then
    port=$opt_p_arg
fi

if ( $opt_w ); then
    for ((i = 1; i <= $opt_w_arg; i++)); do
        echo "Starting client.py No. $i background process"
        python3 worker.py --port $port 2>/dev/null &
    done
fi
